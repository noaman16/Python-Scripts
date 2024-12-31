import os
import subprocess
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import argparse
import difflib

# Generate timestamp
timestamp = datetime.now().strftime('%Y%m%d')

# Initialize a list to hold error messages
error_messages = []

# Capture crontab output for a specific user
def capture_crontab(user):
    try:
        result = subprocess.run(['sudo', '-u', user, 'crontab', '-l'], stdout=subprocess.PIPE, check=True)
        crontab_content = result.stdout.decode('utf-8')
        print(f"Crontab captured for user {user}")
        return crontab_content
    except subprocess.CalledProcessError as e:
        error_message = f"Error capturing crontab for user {user}: {e}"
        print(error_message)
        error_messages.append(error_message)
        return None

# Upload content to S3
def upload_to_s3(content, s3_bucket_name, s3_key):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        s3_path = os.getenv('S3_PATH', '').strip()
        if s3_path:
            s3_key = os.path.join(s3_path, s3_key)
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=content)
        print(f"Crontab content uploaded to S3 bucket {s3_bucket_name} with key {s3_key}")
    except (NoCredentialsError, ClientError) as e:
        error_message = f"Error uploading to S3: {e}"
        print(error_message)
        error_messages.append(error_message)

# Compare backups and send a report
def compare_backups(s3_bucket_name, user, s3_key_today):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        s3_path = os.getenv('S3_PATH', '').strip()
        crontab_backup_filename = os.getenv('CRONTAB_BACKUP_FILENAME')
        if s3_path:
            s3_key_today = os.path.join(s3_path, s3_key_today)
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        s3_key_yesterday = s3_key_today.replace(timestamp, yesterday_date)

        today_content = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key_today)['Body'].read().decode('utf-8')
        try:
            yesterday_content = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key_yesterday)['Body'].read().decode('utf-8')
        except s3_client.exceptions.NoSuchKey:
            send_email(f"New crontab backup created for user {user} on {crontab_backup_filename}", f"Today's crontab backup for user {user} is new and no prior backup exists.")
            return

        added_lines = []
        removed_lines = []
        modified_lines = []

        old_lines = yesterday_content.splitlines()
        new_lines = today_content.splitlines()

        max_lines = max(len(old_lines), len(new_lines))
        

        for line_no in range(max_lines):
            old_line = old_lines[line_no].strip() if line_no < len(old_lines) else None
            new_line = new_lines[line_no].strip() if line_no < len(new_lines) else None

            # Skip blank lines
            if not old_line and not new_line:
                continue

            if old_line is None and new_line:
                added_lines.append(f"Line {line_no + 1}: {new_line}")
            elif old_line and new_line is None:
                removed_lines.append(f"Line {line_no + 1}: {old_line}")
            elif old_line != new_line:
                modified_lines.append(f"Line {line_no + 1}:\n  Old: {old_line}\n  New: {new_line}")


        changes_summary = []
        if added_lines:
            changes_summary.append("*Added Lines:*\n\n" + "\n".join(added_lines) + "\n")
        if removed_lines:
            changes_summary.append("*Removed Lines:*\n\n" + "\n".join(removed_lines) + "\n")
        if modified_lines:
            changes_summary.append("*Modified Lines:*\n\n" + "\n".join(modified_lines) + "\n")

        if changes_summary:
            email_body = "\n\n".join(changes_summary)
            send_email(
                f"Crontab changes detected for user {user} on {crontab_backup_filename}",
                f"Changes found in the crontab for user {user}:\n\n{email_body}"
            )
        else:
            print(f"No changes detected in the crontab for user {user} on {crontab_backup_filename}")
    except Exception as e:
        error_message = f"Error comparing backups: {e}"
        print(error_message)
        error_messages.append(error_message)

# Delete old backups from S3 
def delete_old_backups(s3_bucket_name, delete_backup_days):
    deleted_backups = []
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        s3_path = os.getenv('S3_PATH', '').strip()
        cutoff_date = datetime.now() - timedelta(days=int(delete_backup_days))

        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_path + '/')
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.txt'):
                date_str = key.split('_')[-1].split('.')[0]
                try:
                    backup_date = datetime.strptime(date_str, '%Y%m%d')
                    if backup_date < cutoff_date:
                        print(f"Deleting old backup: {key}")
                        s3_client.delete_object(Bucket=s3_bucket_name, Key=key)
                        deleted_backups.append(key)
                except ValueError:
                    continue
    except Exception as e:
        error_message = f"Error deleting old backups: {e}"
        print(error_message)
        error_messages.append(error_message)

# Send an email notification
def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = os.getenv('EMAIL_SENDER')
        email_to = os.getenv('EMAIL_TO').split(',')
        msg['To'] = ', '.join(email_to)
        msg.attach(MIMEText(body))

        with smtplib.SMTP(os.getenv('EMAIL_HOST'), os.getenv('EMAIL_PORT')) as server:
            server.starttls()
            server.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASSWORD'))
            server.sendmail(os.getenv('EMAIL_SENDER'), email_to, msg.as_string())
        print(f"Email sent: {subject}")
    except Exception as e:
        error_message = f"Failed to send email: {e}"
        print(error_message)
        error_messages.append(error_message)

# Main function
def main(env_file_path):
    load_dotenv(env_file_path)

    crontab_backup_filename = os.getenv('CRONTAB_BACKUP_FILENAME')
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    delete_backup_days = os.getenv('DELETE_BACKUP_DAYS', '1')
    users = os.getenv('USERS')

    if users:
        for user in users.split(','):
            user = user.strip()
            user_crontab_content = capture_crontab(user)
            if user_crontab_content:
                user_s3_key = f"{user}_{crontab_backup_filename}_{timestamp}.txt"
                upload_to_s3(user_crontab_content, s3_bucket_name, user_s3_key)
                compare_backups(s3_bucket_name, user, user_s3_key)

    delete_old_backups(s3_bucket_name, delete_backup_days)

    if error_messages:
        send_email("Crontab Backup Errors", "\n".join(error_messages))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("env_file_path", nargs='?', default=".env")
    args = parser.parse_args()
    main(args.env_file_path)