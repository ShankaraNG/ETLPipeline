import smtplib
from email.message import EmailMessage
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config_loader as cnfloader
import logger as logging

def sendbatchemail(body_text, configmapFilePath):
    try:
        body = f"Hi Team,\n\n{body_text}\n\nThanks and Regards,\nETL Pipeline Tool"
        config = cnfloader.load_properties()
        configMap = cnfloader.load_File_properties(configmapFilePath)
        smtp_host = config.get('emailsmtphost')
        smtp_port = int(config.get('emailsmtpport'))
        subject = config.get('emailsubject')
        recipients = configMap.get('emaildistributionlist')

        if not all([smtp_host, smtp_port, subject, recipients]):
            raise ValueError((400, "Missing Mail COnfigurations"))

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = 'noreply@gmail.com'
        msg['To'] = recipients
        msg.set_content(body)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(msg)
            logging.logger('INFO', 'ETL Pipeline Tool', 200, 'Main has been sent successfully')

    except Exception as e:
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        if code in [400, 404]:
            sys.exit(1)
        return None

