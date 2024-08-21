# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as f
import logging
from pyspark.sql.types import *
import json
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content

# COMMAND ----------

def count_validation(file_name, count_of_records):
    if count_of_records > 0:
        logger.info(f'{file_name} - contains data - so proceeding')
    else:
        raise ValueError(f'FileName : {file_name} does not contain any record.')

# COMMAND ----------

def file_extension_validation(file_name):
    ## fetching the file extension from filename ##
    file_extension = file_name.split(".")[-1].lower()
    if(file_extension == "csv"):
        raise ValueError(f'FileName : {file_name} is in unsupported file format. Supported file format is "csv". ') 
    else:
        logger.info(f"FileName : {file_name} - file format is supported - so proceeding")

# COMMAND ----------

def email_notification(file_name, subject, content):
    api_key = "saved_key_from secret_manager"
    sg = sendgrid.SendGridAPIClient(api_key)
    sender_email = "abc@gmail.com" ## Change to your verified sender ##
    from_email = Email(sender_email)  
    to_email =[To(f"user1@gmail.com"),To(f"user1@gmail.com")]  ## update recipient as required ##
    
    mail = Mail(from_email, to_email, subject, content)

    ## Get a JSON-ready representation of the Mail object ##
    mail_json = mail.get()

    # Send an HTTP POST request to /mail/send ##
    response = sg.client.mail.send.post(request_body=mail_json)
    if response.status_code == '202' or response.status_code == 202:
        logger.info("Email notification sent to the user.")
    else:
        logger.info("Email notification not sent to the user.")

# COMMAND ----------

def main_function(incoming_path, count_of_records, df):
    file_name = incoming_path.split("/")[-1]
    error_path = incoming_path.replace("incoming/","error/")
    archive_path = incoming_path.replace("incoming/","archive/")
    cleaned_records_data = incoming_path.replace("incoming/","cleaned_data/")
    bad_records_data = incoming_path.replace("incoming/","bad_records_data/")
    try:
        count_validation(file_name, count_of_records)
        file_extension_validation(file_name)
        logger.info(f"File validation for {file_name} complete.")

        ## second module to check bad and good records ##
        bad_records = []
        cleaned_data = []

        def clean_phone_number(phone):
            if pd.isnull(phone):
                return None
            ## remove "+" and spaces ##
            phone = phone.replace("+", "").replace(" ", "")
            ## checking if phone number is of 10 digits ##
            if re.fullmatch(r"\d{10}", phone):
                return phone
            else:
                return None

        ## function to clean descriptive fields ##
        def clean_description_field(field):
            if pd.isnull(field):
                return None
            # removing special or junk characters from fields ##
            cleaned_field = re.sub(r"[^a-zA-Z0-9\s,]", "", field)
            return cleaned_field
        
        ## checking each record for issues ##
        for _, row in df.iterrows():
            ## cleaning and validating phone numbers ##
            contact1 = clean_phone_number(row.get("phone1"))
            contact2 = clean_phone_number(row.get("phone2"))

            # checking if first number is null then add to bad records if first is correct then second might be there or might not be, hence checking first number ##
            if pd.isnull(contact1):
                bad_records.append(row)
                continue

            ## checking for null values in required fields ##
            required_fields = ["name", "email", "address"]
            if any(pd.isnull(row[field]) for field in required_fields):
                bad_records.append(row)
                continue

            ## cleaning descriptive fields ##
            address = clean_description_field(row.get("address"))
            reviews_list = clean_description_field(row.get("reviews_list"))

            ## storing cleaned data ##
            cleaned_data.append({
                "name": row.get("name"),
                "email": row.get("email"),
                "contact1": contact1,
                "contact2": contact2,
                "address": address,
                "reviews_list": reviews_list
            })

        ## convert cleaned data and bad data records into dataFrames to be written in csv files ##
        df_cleaned = pd.DataFrame(cleaned_data)
        df_bad_records = pd.DataFrame(bad_records)

        ## save cleaned data to a new CSV file ##
        cleaned_file_path = f'{cleaned_records_data}/cleaned_data.csv'
        df_cleaned.to_csv(cleaned_file_path, index=False)

        # Save bad records to a separate CSV file
        bad_records_file_path = f'{bad_records_data}/bad_records.csv'
        df_bad_records.to_csv(bad_records_file_path, index=False)

        ## archiving the input file into archive bucket so that next day same file is not read and we maintain history ##
        dbutils.fs.mv(incoming_path, archive_path)
        logger.info(f"File archived successfully")

        ## sending success email ##
        subject = f"Uploaded user_file {file_name} updated successfully."
        content = Content("text/plain", f"{str(e)}. \n The file has been cleaned successfully after completing all the tests.")

        email_notification(file_name, subject, content)

    except Exception as e:
        logger.info(f"File validation failed so moving the file :{file_name} to error folder")
        dbutils.fs.mv(incoming_path, error_path)

        ## sending failure email ##
        subject = f"Uploaded user_file {file_name} was rejected"
        content = Content("text/plain", f"{str(e)}. \n Please refer path for the error file with added information : {error_path}")

        email_notification(file_name, subject, content)
        raise e

# COMMAND ----------

incoming_path = f'path of uploaded file'
df = pd.read_csv(incoming_path)
count_of_records = len(df)

main_function(incoming_path, count_of_records, df)