import os
import subprocess
import argparse
from dotenv import load_dotenv
import boto3
from datetime import datetime, timedelta
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def setup_logging(log_file):
    """Setup logging configuration."""
    logging.basicConfig(
        filename=log_file,
        filemode='w',  
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def send_email(subject, body, email_host, email_port, email_user, email_password, email_sender, email_to, log_file):
    """Send email notification."""
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Attach the log file to the email
    with open(log_file, 'rb') as attachment:
        part = MIMEApplication(attachment.read(), Name=os.path.basename(log_file))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(log_file)}"'
    msg.attach(part)
    
    try:
        server = smtplib.SMTP(email_host, email_port)
        server.starttls()
        server.login(email_user, email_password)
        server.send_message(msg)
        server.quit()
        logging.info("Email notification sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email notification: {str(e)}")

def backup_task(task_path, task_name, backup_path, aws_access_key, aws_secret_key, aws_region, s3_bucket_name, upload_to_taskscheduler, email_host, email_port, email_user, email_password, email_sender, email_to, log_file, folder_name, s3_backup_folder, schtasks):
    # Build the full task path
    full_task_path = os.path.join(task_path, task_name)

    # Build the command to export the task
    export_command = f'"{schtasks}" /Query /TN "{full_task_path}" /XML > "{backup_path}{task_name}.xml"'

    # Execute the command
    logging.info(f"Exporting task: {full_task_path}")
    result = subprocess.run(export_command, shell=True, capture_output=True)
    if result.returncode != 0:
        error_message = result.stderr.decode('utf-8')
        logging.error(f"Failed to export task: {error_message}")
        return False

    # Upload the backup file to S3
    datestamp = datetime.now().strftime('%Y%m%d')
    s3_filename = f'{task_name}_{datestamp}.xml'

    if upload_to_taskscheduler.lower() == "yes":
        s3_key = f"{s3_backup_folder}/{s3_filename}"
    else:
        s3_key = s3_filename

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    try:
        s3_client.upload_file(f'{backup_path}{task_name}.xml', s3_bucket_name, s3_key)
        logging.info(f"Uploaded backup file to S3: s3://{s3_bucket_name}/{s3_key}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload backup file to S3: {str(e)}")
        return False

def delete_old_files(s3_bucket_name, aws_access_key, aws_secret_key, delete_days, upload_to_taskscheduler, folder_names, s3_backup_folder):
    """Delete files from S3 bucket older than specified days."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        # Calculate date threshold for deletion
        cutoff_date = datetime.now() - timedelta(days=delete_days)

        response = s3_client.list_objects_v2(Bucket=s3_bucket_name)
        for obj in response.get('Contents', []):
            filename = obj['Key']
            if filename.endswith('.xml'):
                date_str = filename.split('_')[-1].split('.')[0]
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    if file_date < cutoff_date:
                        # Check if the file is in the specified backup folder
                        if upload_to_taskscheduler.lower() == "yes" and s3_backup_folder in filename:
                            s3_client.delete_object(Bucket=s3_bucket_name, Key=filename)
                            logging.info(f"Deleted old file: s3://{s3_bucket_name}/{filename}")
                        elif upload_to_taskscheduler.lower() == "no":
                            s3_client.delete_object(Bucket=s3_bucket_name, Key=filename)
                            logging.info(f"Deleted old file: s3://{s3_bucket_name}/{filename}")
                except ValueError:
                    continue

    except Exception as e:
        logging.error(f"Failed to delete old files from S3: {str(e)}")


def log_and_backup_tasks_in_folder(folder_name, backup_path, aws_access_key, aws_secret_key, aws_region, s3_bucket_name, upload_to_taskscheduler, email_host, email_port, email_user, email_password, email_sender, email_to, log_file, schtasks, ignored_job_names, s3_backup_folder):
    """Log and backup all tasks within the specified folder and its subfolders."""
    try:
        result = subprocess.run([schtasks, "/Query", "/FO", "LIST", "/V"], capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"Failed to query tasks: {result.stderr}")
            return False
        
        all_tasks_info = result.stdout
        logging.info(f"All Task Scheduler tasks and their details within folder '{folder_name}':")
        
        tasks = []
        current_task = {}
        for line in all_tasks_info.splitlines():
            if line.startswith("HostName:"):
                if current_task:
                    tasks.append(current_task)
                    current_task = {}
            if line:
                key, _, value = line.partition(":")
                current_task[key.strip()] = value.strip()
        if current_task:
            tasks.append(current_task)

        for task in tasks:
            task_name = task.get("TaskName", "").strip("\\")
            if task_name.startswith(folder_name):
                task_path = "\\".join(task_name.split("\\")[:-1])
                task_name_only = task_name.split("\\")[-1]
                if task_name_only in ignored_job_names:
                    logging.info(f"Ignoring Task: {task_name}")
                    continue
                logging.info(f"Task: {task_name}")
                backup_task(task_path, task_name_only, backup_path, aws_access_key, aws_secret_key, aws_region, s3_bucket_name, upload_to_taskscheduler, email_host, email_port, email_user, email_password, email_sender, email_to, log_file, folder_name, s3_backup_folder, schtasks)
        
    except Exception as e:
        logging.error(f"An error occurred while logging and backing up tasks: {str(e)}")
        return False
    
def compare_backups_and_notify(s3_bucket_name, s3_backup_folder, aws_access_key, aws_secret_key, aws_region, email_host, email_port, email_user, email_password, email_sender, email_to, log_file):
    """Compare today's backups with yesterday's backups and send email notifications."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    # Get today's and yesterday's dates
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    try:
        # List objects in the S3 bucket
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_backup_folder)
        if 'Contents' not in response:
            logging.warning(f"No files found in S3 bucket: {s3_bucket_name}/{s3_backup_folder}")
            return

        files_by_date = {'today': [], 'yesterday': []}

        for obj in response['Contents']:
            filename = obj['Key']
            if today in filename:
                # Extract task name without the date or timestamp, preserving file extension
                task_name = filename.split(f"_{today}")[0]  # Remove the date suffix
                files_by_date['today'].append(f"{task_name}{filename.split(f'_{today}')[1]}")  # Add the extension back
            elif yesterday in filename:
                # Extract task name without the date or timestamp, preserving file extension
                task_name = filename.split(f"_{yesterday}")[0]  # Remove the date suffix
                files_by_date['yesterday'].append(f"{task_name}{filename.split(f'_{yesterday}')[1]}")  # Add the extension back

        # Determine new files and deleted files based on task name
        today_files = set(files_by_date['today'])
        yesterday_files = set(files_by_date['yesterday'])

        new_files = today_files - yesterday_files
        deleted_files = yesterday_files - today_files

        email_body = ""
        if new_files:
            email_body += "New Task Scheduler Jobs Found:\n"
            email_body += "\n".join(new_files) + "\n\n"
        if deleted_files:
            email_body += "Deleted Task Scheduler Jobs:\n"
            email_body += "\n".join(deleted_files) + "\n\n"

        # Send email notification if there are any changes
        if email_body:
            send_email(
                subject="Task Scheduler Backup Changes Detected",
                body=email_body,
                email_host=email_host,
                email_port=email_port,
                email_user=email_user,
                email_password=email_password,
                email_sender=email_sender,
                email_to=email_to,
                log_file=log_file
            )
            logging.info("Email sent regarding changes in Task Scheduler backups.")
        else:
            logging.info("No changes detected in Task Scheduler backups.")
    except Exception as e:
        logging.error(f"Failed to compare backups or send notification: {str(e)}")

   
def main(env_file_path):
    # Load environment variables from the specified file
    load_dotenv(env_file_path)

    # Retrieve folder name, backup path, ignored job names, and other configurations from environment variables
    folder_names = os.getenv("TASK_SCHEDULER_FOLDERS").split(',')
    ignored_job_names = os.getenv("IGNORED_JOB_NAMES", "").split(',')
    backup_path = os.getenv("BACKUP_PATH")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    aws_region = os.getenv("AWS_REGION")
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")
    delete_days = int(os.getenv("DELETE_DAYS"))
    s3_backup_folder = os.getenv("S3_BACKUP_FOLDER")
    schtasks = os.getenv("PATH_OF_SCHTASKS")
    upload_to_taskscheduler = os.getenv("UPLOAD_TO_TASKSCHEDULER", "yes")

    # Email configuration
    email_host = os.getenv("EMAIL_HOST")
    email_port = int(os.getenv("EMAIL_PORT", 587))
    email_user = os.getenv("EMAIL_USER")
    email_password = os.getenv("EMAIL_PASSWORD")
    email_sender = os.getenv("EMAIL_SENDER")
    email_to = os.getenv("EMAIL_TO")

    # Setup logging
    log_file = os.path.join(backup_path, "taskchedulerscript.log")
    setup_logging(log_file)

    # Log and backup tasks in each specified folder
    for folder_name in folder_names:
        log_and_backup_tasks_in_folder(folder_name.strip(), backup_path, aws_access_key, aws_secret_key, aws_region, s3_bucket_name, upload_to_taskscheduler, email_host, email_port, email_user, email_password, email_sender, email_to, log_file, schtasks, ignored_job_names, s3_backup_folder)

    # Delete old files from the S3 bucket
    delete_old_files(s3_bucket_name, aws_access_key, aws_secret_key, delete_days, upload_to_taskscheduler, folder_names, s3_backup_folder)

    # Compare backups and notify for changes
    compare_backups_and_notify(
        s3_bucket_name=s3_bucket_name,
        s3_backup_folder=s3_backup_folder,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_region=aws_region,
        email_host=email_host,
        email_port=email_port,
        email_user=email_user,
        email_password=email_password,
        email_sender=email_sender,
        email_to=email_to,
        log_file=log_file
    )


if __name__ == "__main__":
    # Creating an argument parser
    parser = argparse.ArgumentParser()

    # Adding arguments
    parser.add_argument("env_file_path", help="Path to the environment file")

    # Parsing the arguments
    args = parser.parse_args()

    # Execute main code
    main(args.env_file_path)