from email.mime.text import MIMEText
import email.utils
import smtplib
from typing import List

# Constants
SMTP_SERVER = 'mail.vgt.vito.be'
SENDER_NAME = 'WEED openEO processing cluster'
SENDER_EMAIL = 'esa.weed.project@vito.be'


def format_email_address(name_email_pair: tuple[str, str]) -> str:
    """
    Formats a name and email address pair into a properly formatted email address.

    :param name_email_pair: Tuple containing a name and an email address. The first element
        of the tuple represents the name as a string, and the second element represents
        the email address as a string.
    :return: A properly formatted email address as a string.
    """
    return email.utils.formataddr(name_email_pair)

def send_email(recipient: tuple[str, str] | str, subject: str, msg_text: str, debug_flag: bool = False) -> None:
    """
    Sends an email message using an SMTP server.
    :param recipient: The recipient's information. Can be either:
        - A list of name and email as a tuple, e.g., ("Name", "email@example.com"), or
        - A file path as a string, where the recipient list is stored.
    :param subject: The subject line of the email.
    :param msg_text: The message text to be sent in the email body.
    :param debug_flag: A flag to enable or disable debug information for SMTP communication.
        Defaults to False.
    """
    sender = (SENDER_NAME, SENDER_EMAIL)

    # Extract recipient list if a file path is provided
    recipients = (
        read_recipients_from_file(recipient) if isinstance(recipient, str) else [recipient]
    )

    for recipient_entry in recipients:
        # Create the message for each recipient
        msg = MIMEText(msg_text)
        msg['To'] = format_email_address(recipient_entry)
        msg['From'] = format_email_address(sender)
        msg['Subject'] = subject

        # Send the message using the SMTP server
        with smtplib.SMTP(SMTP_SERVER) as server:
            server.set_debuglevel(debug_flag)  # show communication with the server
            server.sendmail(SENDER_EMAIL, [recipient_entry[1]], msg.as_string())

def read_recipients_from_file(file_path: str) -> List[tuple[str, str]]:
    """
    Reads recipients from a file, where each line contains a name and email separated by a semicolon (';').
    :param file_path: Path to the file containing recipients.
    :return: A list of tuples, where each tuple contains (name, email).
    """
    recipients = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                stripped_line = line.strip()
                if not stripped_line:  # Skip empty lines
                    continue
                try:
                    name, email = stripped_line.split(';')  # Split by semicolon
                    recipients.append((name.strip(), email.strip()))  # Add formatted name and email
                except ValueError as e:
                    print(f"Skipping malformed line: {line.strip()}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    return recipients
