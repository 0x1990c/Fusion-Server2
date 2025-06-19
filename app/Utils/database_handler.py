from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, update, cast, Date, distinct, or_, and_
from app.Model.DatabaseModel import Message, Project, MessageHistory, Report, User, Variables, Status, Customer, CustomerCategory, Phone, Case, Courts, Counties, Template, CourtOwner, ShortCodes, Fields
from datetime import datetime
from app.Model.MainTable import MainTableModel, TemplateModel, TemplateCaseModel
from app.Model.CaseModel import TimeRange
from app.Model.CaseModel import FilterCondition, ShortcodeModel
from sqlalchemy import delete
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import base64
import re
from collections import defaultdict

# Utility Functions for Asynchronous Execution

# Main Table Retrieval
async def get_main_table(db: AsyncSession):
    stmt = (
        select(
            Message.id,
            Message.last_message,
            Message.message_status,
            Message.qued_timestamp,
            Message.sent_timestamp,
            Message.sent_success,
            Message.image_url,
            Message.categories,
            Message.phone_numbers,
            Message.num_sent,
            Message.created_at
        ).order_by(Message.created_at.desc())
    )
    result = await db.execute(stmt)
    return result.all()

async def get_message_num_sent(db: AsyncSession, current_time: datetime):
    stmt = select(Message.id, Message.num_sent).filter(
        func.json_length(Message.phone_numbers) > Message.num_sent,
        Message.qued_timestamp < current_time
    )
    
    result = await db.execute(stmt)
    return result.all()

# Message CRUD Operations
async def get_message(db: AsyncSession, message_id: int):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def find_message_with_phone(db: AsyncSession, phone: str):
    phone = phone.replace(" ", "")
    stmt = select(Message).filter(func.replace(Message.phone, ' ', '') == phone)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def insert_message(db: AsyncSession, item: MainTableModel):
    new_message = Message(
        last_message=item.last_message,
        message_status=item.message_status,
        qued_timestamp=item.qued_timestamp,
        sent_timestamp=item.sent_timestamp,
        sent_success=item.sent_success,
        image_url=item.image_url,
        categories=item.categories,
        num_sent=item.num_sent,
        phone_numbers=item.phone_numbers,
        created_at=item.created_at
    )
    db.add(new_message)
    await db.commit()
    await db.refresh(new_message)
    return new_message

async def update_message(db: AsyncSession, message_id: int, item: MainTableModel):
    
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message:
        message.last_message = item.last_message
        message.message_status = item.message_status
        message.qued_timestamp = item.qued_timestamp
        message.sent_timestamp = item.sent_timestamp
        message.sent_success = item.sent_success
        message.image_url = item.image_url
        message.categories = item.categories
        message.num_sent = item.num_sent
        message.created_at = item.created_at
        await db.commit()
        await db.refresh(message)
        return message
    return None

async def update_message_status(db: AsyncSession, message_id: int, status: int):
    stmt = update(Message).filter(Message.id == message_id).values(message_status=status)
    await db.execute(stmt)
    await db.commit()
    return True

async def delete_message(db: AsyncSession, message_id: int):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message:
        await db.delete(message)
        await db.commit()
        return True
    return False

async def restore_message(db: AsyncSession, message_id: int):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message:
        message.is_deleted = 0
        await db.commit()
        return message
    return None

async def update_sending_method(db: AsyncSession, message_id: int, method: int):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message:
        message.sending_method = method
        await db.commit()
        return message
    return None

async def update_opt_in_status_email(db: AsyncSession, message_id: int, opt_in_status_email: int):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message:
        message.opt_in_status_email = opt_in_status_email
        await db.commit()
        return message
    return None

async def update_opt_in_status_phone(db: AsyncSession, phone_number: str, opt_in_status: int):

    print("update_opt_in_status_phone")

    stmt = update(Phone).filter(Phone.phone_number == phone_number).values(opt_in_status=opt_in_status, back_timestamp=datetime.utcnow())

    try:
        await db.execute(stmt)
        await db.commit()
        print("True")
        return True
    except SQLAlchemyError as e:
        await db.rollback()  # Rollback the transaction on error
        print("False")
        print(f"Error occurred: {e}")  # Log the error
        return False

async def update_opt_in_status_sent_timestamp(db: AsyncSession, phone_id: int):
    stmt = select(Phone).filter(Phone.id == phone_id)
    result = await db.execute(stmt)
    phone = result.scalar_one_or_none()

    print("test: phone ", phone);

    if phone:
        phone.sent_timestamp = datetime.utcnow()
        await db.commit()
        await db.refresh(phone)
        return phone
    return None
# Project CRUD Operations
async def get_project(db: AsyncSession, project_id: int):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def insert_project(db: AsyncSession, claim_number: str, customer_id: int, project_name: str):
    stmt = select(Project).filter_by(claim_number=claim_number, customer_id=customer_id)
    result = await db.execute(stmt)
    existing_project = result.scalar_one_or_none()

    if existing_project is None:
        new_project = Project(
            claim_number=claim_number,
            customer_id=customer_id,
            project_name=project_name,
        )
        db.add(new_project)
        await db.commit()
        await db.refresh(new_project)
        return new_project
    return existing_project

async def update_project(db: AsyncSession, project_id: int, **kwargs):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    project = result.scalar_one_or_none()

    if project:
        for key, value in kwargs.items():
            setattr(project, key, value)
        await db.commit()
        return project
    return None

async def delete_project(db: AsyncSession, project_id: int):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    project = result.scalar_one_or_none()

    if project:
        await db.delete(project)
        await db.commit()
        return True
    return False

# MessageHistory CRUD Operations
async def get_message_history(db: AsyncSession, message_history_id: int):
    stmt = select(MessageHistory).filter(MessageHistory.id == message_history_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_all_message_history(db: AsyncSession):
    stmt = select(MessageHistory)
    result = await db.execute(stmt)
    return result.scalars().all()

async def create_message_history(db: AsyncSession, message: str, project_id: int):
    new_message_history = MessageHistory(message=message, project_id=project_id, sent_time=datetime.utcnow())
    db.add(new_message_history)
    await db.commit()
    await db.refresh(new_message_history)
    return new_message_history

async def update_message_history(db: AsyncSession, message_history_id: int, message: str):
    stmt = select(MessageHistory).filter(MessageHistory.id == message_history_id)
    result = await db.execute(stmt)
    message_history = result.scalar_one_or_none()

    if message_history:
        message_history.message = message
        await db.commit()
        return message_history
    return None

async def delete_message_history(db: AsyncSession, message_history_id: int):
    stmt = select(MessageHistory).filter(MessageHistory.id == message_history_id)
    result = await db.execute(stmt)
    message_history = result.scalar_one_or_none()

    if message_history:
        await db.delete(message_history)
        await db.commit()
        return True
    return False

# Report CRUD Operations
async def insert_report(db: AsyncSession, project_id: int, message: str = "", timestamp: str = ""):
    if not timestamp:
        timestamp = datetime.utcnow().isoformat()

    stmt = select(Report).filter_by(project_id=project_id, message=message, timestamp=timestamp)
    result = await db.execute(stmt)
    existing_report = result.scalar_one_or_none()

    if existing_report is None:
        new_report = Report(project_id=project_id, message=message, timestamp=timestamp)
        db.add(new_report)
        await db.commit()
        await db.refresh(new_report)
        return new_report
    return existing_report

async def update_report(db: AsyncSession, report_id: int, message: str):
    stmt = select(Report).filter(Report.id == report_id)
    result = await db.execute(stmt)
    report = result.scalar_one_or_none()

    if report:
        report.message = message
        await db.commit()
        return report
    return None

async def delete_report(db: AsyncSession, report_id: int):
    stmt = select(Report).filter(Report.id == report_id)
    result = await db.execute(stmt)
    report = result.scalar_one_or_none()

    if report:
        await db.delete(report)
        await db.commit()
        return True
    return False

# User CRUD Operations
async def get_user(db: AsyncSession, user_id: int):
    stmt = select(User).filter(User.id == user_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_user_by_email(db: AsyncSession, email: str):
    stmt = select(User).filter(User.username == email)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_user_by_email_approved(db: AsyncSession, email: str):
    stmt = select(User).filter(User.username == email)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    print("sigin user1: ", user.approved);
    if user: 
        print("sigin user name: ", user.username);
        if user.approved == 1:
            print("sigin user2: ", user.approved);
            return user
        elif user.approved == 0:
            return False
    return None

async def create_user(db: AsyncSession, username: str, password: str, forgot_password_token: str, approved: int):
    new_user = User(username=username, password=password, forgot_password_token=forgot_password_token, approved=approved, user_type = 0, sms_balance = 500)
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    return new_user

async def update_user(db: AsyncSession, user_id: int, **kwargs):
    stmt = select(User).filter(User.id == user_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if user:
        for key, value in kwargs.items():
            setattr(user, key, value)
        await db.commit()
        return user
    return None

async def delete_user(db: AsyncSession, user_id: int):
    stmt = select(User).filter(User.id == user_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if user:
        await db.delete(user)
        await db.commit()
        return True
    return False

# Variables CRUD Operations
async def get_variables(db: Session):
    # stmt = select(Variables)
    stmt = select(Variables).filter(Variables.id == 1)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def create_variables(db: AsyncSession):
    new_variables = Variables(openAIKey="", twilioPhoneNumber="", twilioAccountSID="", twilioAuthToken="", sendgridEmail="", sendgridApiKey="", optin_message="", timer=0)
    db.add(new_variables)
    await db.commit()
    await db.refresh(new_variables)
    return new_variables

async def update_variables(db: AsyncSession, variables_id: int, **kwargs):
    stmt = select(Variables).filter(Variables.id == variables_id)
    result = await db.execute(stmt)
    variables = result.scalar_one_or_none()

    if variables:
        for key, value in kwargs.items():
            setattr(variables, key, value)
        await db.commit()
        return variables
    return None

async def delete_variables(db: AsyncSession, variables_id: int):
    stmt = select(Variables).filter(Variables.id == variables_id)
    result = await db.execute(stmt)
    variables = result.scalar_one_or_none()

    if variables:
        await db.delete(variables)
        await db.commit()
        return True
    return False

async def set_project_message(db: AsyncSession, project_id: int, message: str):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    project = result.scalar_one_or_none()

    if project:
        project.last_message = message
        await db.commit()
        return project
    return None

async def insert_message_history(db: AsyncSession, message: str, project_id: int):
    new_message_history = MessageHistory(message=message, project_id=project_id, sent_time=datetime.utcnow())
    db.add(new_message_history)
    await db.commit()
    await db.refresh(new_message_history)
    return new_message_history

async def get_message_history_by_project_id_as_list(db: AsyncSession, project_id: int):
    stmt = select(MessageHistory).filter(MessageHistory.project_id == project_id)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_message_history_by_project_id(db: AsyncSession, project_id: int):
    stmt = select(MessageHistory).filter(MessageHistory.project_id == project_id)
    result = await db.execute(stmt)
    messages = result.scalars().all()

    formatted_messages = ''
    print("messages --: ", messages)
    for message in messages:
        formatted_messages += f"{message.sent_time}\n{message.message}\n---------------\n"
    return formatted_messages.rstrip('---------------\n')

async def get_message_history_by_history_id(db: AsyncSession, history_id: int):
    stmt = select(MessageHistory).filter(MessageHistory.id == history_id)
    result = await db.execute(stmt)
    messages = result.scalars().all()

    formatted_messages = ''
    print("messages --: ", messages)
    for message in messages:
        formatted_messages += f"{message.sent_time}\n{message.message}\n---------------\n"
    return formatted_messages.rstrip('---------------\n')

async def get_message_history_by_customer_id(db: AsyncSession, customer_id: int):
    stmt = select(Project).filter(Project.customer_id == customer_id)
    result = await db.execute(stmt)
    projects = result.scalars().all()

    formatted_messages = ''
    for project in projects:
        project_messages = await get_message_history_by_project_id(db, project.id)
        formatted_messages += project_messages
    return formatted_messages.rstrip('---------------\n')

async def set_project_status(db: AsyncSession, project_id: int, message_status: int, qued_timestamp):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    project = result.scalar_one_or_none()

    if project:
        project.message_status = message_status
        project.qued_timestamp = qued_timestamp
        await db.commit()
        return project
    return None

async def set_project_sent(db: AsyncSession, project_id: int, message_status: int, sent_timestamp):
    stmt = select(Project).filter(Project.id == project_id)
    result = await db.execute(stmt)
    project = result.scalar_one_or_none()

    if project:
        project.message_status = message_status
        project.sent_timestamp = sent_timestamp
        await db.commit()
        return project
    return None

async def get_reports_by_project_id(db: AsyncSession, project_id: int):
    stmt = select(Report).filter(Report.project_id == project_id)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_all_projects(db: AsyncSession):
    stmt = select(Project)
    result = await db.execute(stmt)
    return result.scalars().all()

async def check_duplicate_message(db: AsyncSession, message: str):
    stmt = select(MessageHistory).filter(MessageHistory.message == message)
    result = await db.execute(stmt)
    return result.scalar_one_or_none() is not None

async def update_sent_status(db: AsyncSession, message_id: int, sent_success: bool):
    stmt = select(Message).filter(Message.id == message_id)
    result = await db.execute(stmt)
    message = result.scalar_one_or_none()

    if message and sent_success:
        message.num_sent += 1
        await db.commit()
        await db.refresh(message)
        return message
    return None

async def set_db_update_status(db: AsyncSession, status_id: int, db_update_status: int):
    stmt = select(Status).filter(Status.id == status_id)
    result = await db.execute(stmt)
    status = result.scalar_one_or_none()

    if status:
        status.db_update_status = db_update_status
        await db.commit()
        return status
    return None

# Status CRUD Operations
async def get_status(db: AsyncSession):
    stmt = select(Status)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def update_status(db: AsyncSession, status_id, **kwargs):
    stmt = select(Status).filter(Status.id == status_id)
    result = await db.execute(stmt)
    status = result.scalar_one_or_none()

    if status:
        for key, value in kwargs.items():
            setattr(status, key, value)
        await db.commit()
        return status
    return None

async def update_rerun_status(db: AsyncSession, status_id, project_total, project_current):
    stmt = select(Status).filter(Status.id == status_id)
    result = await db.execute(stmt)
    status = result.scalar_one_or_none()

    if status:
        status.project_total = project_total
        status.project_current = project_current
        await db.commit()
        return status
    return None

async def create_status(db: AsyncSession):
    new_status = Status(db_update_status=0, buildertrend_total=0, buildertrend_current=0, xactanalysis_total=0, xactanalysis_current=0, project_total=0, project_current=0)
    db.add(new_status)
    await db.commit()
    await db.refresh(new_status)
    return new_status


# Add new helper methods for phone number operations
async def add_phone_number(db: AsyncSession, customer_id: int, phone_number: str):
    """Add a new phone number to customer's phone numbers list"""
    stmt = select(Customer).filter(Customer.id == customer_id)
    result = await db.execute(stmt)
    customer = result.scalar_one_or_none()
    
    if customer:
        if not customer.phone_numbers:
            customer.phone_numbers = []
        if phone_number not in customer.phone_numbers:
            customer.phone_numbers.append(phone_number)
            await db.commit()
        return customer
    return None

async def remove_phone_number(db: AsyncSession, customer_id: int, phone_number: str):
    """Remove a phone number from customer's phone numbers list"""
    stmt = select(Customer).filter(Customer.id == customer_id)
    result = await db.execute(stmt)
    customer = result.scalar_one_or_none()
    
    if customer and customer.phone_numbers:
        if phone_number in customer.phone_numbers:
            customer.phone_numbers.remove(phone_number)
            await db.commit()
        return customer
    return None

# Customer CRUD Operations
async def insert_customer(db: AsyncSession, phone_numbers: list, categories: list):

    """Insert a new customer with a list of phone numbers and categories"""
    new_customer = Customer(
        phone_numbers=phone_numbers,  # List of phone numbers
        categories=categories  # List of categories
    )
    db.add(new_customer)
    
    await db.commit()
    await db.refresh(new_customer)
    return new_customer

async def update_customer(db: AsyncSession, customer_id: int, phone_numbers: list, categories: list):
    """Update customer's phone numbers and categories lists"""
    stmt = select(Customer).filter(Customer.id == customer_id)
    result = await db.execute(stmt)
    customer = result.scalar_one_or_none()
    
    if customer:
        customer.phone_numbers = phone_numbers
        customer.categories = categories
        await db.commit()
        return customer
    return None


async def delete_customer(db: AsyncSession, customer_id: int):
    stmt = select(Customer).filter(Customer.id == customer_id)
    result = await db.execute(stmt)
    customer = result.scalar_one_or_none()

    if customer:
        await db.delete(customer)
        await db.commit()
        return True
    return False

async def get_customer(db: AsyncSession, customer_id: int):
    stmt = select(Customer).filter(Customer.id == customer_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_customer_table(db: AsyncSession):
    """Get all customers with their id, phone numbers and categories"""
    stmt = select(
        Customer.id,
        Customer.phone_numbers,
        Customer.categories
    )
    result = await db.execute(stmt)
    return result.all()

async def get_customer_categories(db: AsyncSession):
    stmt = select(CustomerCategory)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_customer_category_by_name(db: AsyncSession, name: str):
    stmt = select(CustomerCategory).where(CustomerCategory.name == name)
    result = await db.execute(stmt)
    category = result.scalar_one_or_none()
    return category

async def add_customer_category(db: AsyncSession, name: str):
    new_category = CustomerCategory(name=name)
    db.add(new_category)
    await db.commit()
    await db.refresh(new_category)
    return new_category

async def update_customer_category(db: AsyncSession, customer_category_id: int, name: str):
    stmt = select(CustomerCategory).filter(CustomerCategory.id == customer_category_id)
    result = await db.execute(stmt)
    category = result.scalar_one_or_none()

    if category:
        category.name = name
        await db.commit()
        return category
    return None

async def delete_customer_category(db: AsyncSession, customer_category_id: int):
    stmt = select(CustomerCategory).filter(CustomerCategory.id == customer_category_id)
    result = await db.execute(stmt)
    category = result.scalar_one_or_none()

    if category:
        await db.delete(category)
        await db.commit()
        return True
    return False

async def delete_customer_categories(db: AsyncSession, customer_category_ids: list):
    # Fetch categories that exist in the database
    stmt = select(CustomerCategory).filter(CustomerCategory.id.in_(customer_category_ids))
    result = await db.execute(stmt)
    categories = result.scalars().all()

    if not categories:
        return False  # No categories found to delete

    # Delete each category
    for category in categories:
        await db.delete(category)

    await db.commit()  # Commit the transaction
    return True

async def get_phone_table(db: AsyncSession):
    # Build optimized SQL query with proper table aliases and explicit column selection
    stmt = text("""
        SELECT 
            D.id AS id, 
            D.phone_number AS phone_number, 
            E.status AS optin_status, 
            D.sent_timestamp AS sent_timestamp, 
            D.back_timestamp AS back_timestamp, 
            D.customer_id AS customer_id,
            GROUP_CONCAT(C.name SEPARATOR ', ') AS categories
        FROM
            tbl_phone D
        LEFT JOIN
            tbl_optin_status E ON D.opt_in_status = E.id
        LEFT JOIN 
            tbl_customer_category C 
        ON JSON_CONTAINS((SELECT categories FROM tbl_customer B WHERE B.id = D.customer_id), CAST(C.id AS JSON), '$')
        GROUP BY
            D.id;

    """)
    
    try:
        # Execute query and fetch all results
        result = await db.execute(stmt)
        rows = result.all()
        columns = result.keys()
        # Convert result to list of dictionaries for easier handling
        formatted_result = [
            dict(zip(columns, row))
            for row in rows
        ]
        return formatted_result
        
    except Exception as e:
        # Log error and re-raise
        print(f"Error executing phone table query: {str(e)}")
        raise

# Phone CRUD Operations
async def get_phone(db: AsyncSession, phone_id: int):
    """Get a phone record by ID"""
    stmt = select(Phone).filter(Phone.id == phone_id)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_phone_by_number(db: AsyncSession, phone_number: str):
    """Get a phone record by phone number"""
    stmt = select(Phone).filter(Phone.phone_number == phone_number)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()

async def get_phones_by_customer(db: AsyncSession, customer_id: int):
    """Get all phone records for a customer"""
    stmt = select(Phone).filter(Phone.customer_id == customer_id)
    result = await db.execute(stmt)
    return result.scalars().all()

async def create_phone(db: AsyncSession, phone_number: str, customer_id: int, opt_in_status: int = None):
    
    """Create a new phone record"""
    new_phone = Phone(
        phone_number=phone_number,
        customer_id=customer_id,
        opt_in_status=opt_in_status
    )
    db.add(new_phone)
    await db.commit()
    await db.refresh(new_phone)
    return new_phone

async def update_phone(db: AsyncSession, phone_id: int, **kwargs):
    """Update a phone record"""
    stmt = select(Phone).filter(Phone.id == phone_id)
    result = await db.execute(stmt)
    phone = result.scalar_one_or_none()

    if phone:
        for key, value in kwargs.items():
            setattr(phone, key, value)
        await db.commit()
        return phone
    return None

async def delete_phone(db: AsyncSession, phone_id: int):
    """Delete a phone record"""
    stmt = select(Phone).filter(Phone.id == phone_id)
    result = await db.execute(stmt)
    phone = result.scalar_one_or_none()

    if phone:
        await db.delete(phone)
        await db.commit()
        return True
    return False
    
async def get_optin_message(db: AsyncSession):
    """Get the opt-in message from the Variables table"""
    # stmt = select(Variables)
    stmt = select(Variables).filter(Variables.id == 1)
    result = await db.execute(stmt)
    variables = result.scalar_one_or_none()
    
    print("variables: ", variables.optin_message)
    if variables:
        return variables.optin_message
    return None

async def update_optin_message(db: AsyncSession, new_message: str):
    """Update the opt-in message in the Variables table"""
    stmt = update(Variables).values(optin_message=new_message)
    await db.execute(stmt)
    await db.commit()
    return None

async def update_usertype(db: AsyncSession, email: str, user_type: int):
    stmt = update(User).values(user_type=user_type).where(User.username == email)
    await db.execute(stmt)
    await db.commit()
    return None


async def update_sms_balance(db: AsyncSession, email: str, add_balance: int):
    prev_balance = await get_sms_balance(db, email)
    stmt = update(User).values(sms_balance=prev_balance + add_balance).where(User.username == email)
    await db.execute(stmt)
    await db.commit()
    return None

async def get_sms_balance(db: AsyncSession, email: str):
    stmt = select(User).where(User.username == email)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    return user.sms_balance

async def get_cases(db: AsyncSession, timeRange: TimeRange):
    fromDate = timeRange.fromDate.strftime("%m/%d/%Y")
    toDate = timeRange.toDate.strftime("%m/%d/%Y")

    stmt = (
        select(
            Case.id,
            Case.CaseCategoryKey,
            Case.CaseCategoryGroup,
            Case.CaseNumber,
            Case.Court,
            Case.CourtCode,
            Case.CaseStatus,
            Case.CaseStatusDate,
            Case.CaseType,
            Case.Style,
            Case.DefendantAddressCity
        )
        .filter(
            Case.CaseStatusDate >= fromDate,
            Case.CaseStatusDate <= toDate
        )
        .order_by(Case.CaseType)  # 👈 Sort here
    )

    result = await db.execute(stmt)
    rows = result.all()
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]

async def get_counties(db: AsyncSession,  timeRange: TimeRange):

    fromDate = timeRange.fromDate.strftime("%m/%d/%Y")
    toDate = timeRange.toDate.strftime("%m/%d/%Y")

    stmt = select(
        Case.id,
        Case.Court
    ).distinct().filter(
        Case.Court != "",
        Case.CaseStatusDate >= fromDate,
        Case.CaseStatusDate <= toDate
    )

    result = await db.execute(stmt)
    rows = result.all()
    columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]


async def get_data(db: AsyncSession, filterCondition: FilterCondition):
    fromDate = filterCondition.fromDate.strftime("%m/%d/%Y")
    toDate = filterCondition.toDate.strftime("%m/%d/%Y")
    offset = filterCondition.offset
    selectedCaseTypes = filterCondition.selectedCaseTypes
    selectedCourt = filterCondition.selectedCourt
    selectedCounty = filterCondition.selectedCounty
    username = filterCondition.username

    # Step 1: Get user ID
    user_stmt = select(User.id).where(User.username == username)
    user_result = await db.execute(user_stmt)
    user_id = user_result.scalar()

    if user_id is None:
        return {"total_count": 0, "data": []}

    # Step 2: Get list of court identifiers owned by user
    court_stmt = select(CourtOwner.court).where(CourtOwner.user == user_id)
    court_result = await db.execute(court_stmt)
    user_courts = court_result.scalars().all()

    if not user_courts:
        return {"total_count": 0, "data": []}

    # Step 2.5: Translate selectedCounty (names) to identifiers
    county_ids = []
    if selectedCounty:
        county_stmt = select(Counties.identifier).where(Counties.county.in_(selectedCounty))
        county_result = await db.execute(county_stmt)
        county_ids = county_result.scalars().all()

    # Step 3: Build filters
    filters = [
        Case.CaseStatusDate >= fromDate,
        Case.CaseStatusDate <= toDate,
        or_(*[Case.CaseNumber.contains(court) for court in user_courts])
    ]

    if selectedCaseTypes:
        filters.append(Case.CaseType.in_(selectedCaseTypes))
    if selectedCourt:
        filters.append(Case.Court.in_(selectedCourt))

    if county_ids:
        filters.append(
            or_(*[Case.CaseNumber.startswith(identifier) for identifier in county_ids])
        )

    # Step 4: Get total count
    count_stmt = select(func.count()).select_from(Case).filter(*filters)
    count_result = await db.execute(count_stmt)
    total_count = count_result.scalar()

    # Step 5: Get paginated result
    stmt = select(Case).filter(*filters).limit(100).offset(offset * 100)
    result = await db.execute(stmt)
    rows = result.all()
    columns = result.keys()

    return {
        "total_count": total_count,
        "data": [dict(zip(columns, row)) for row in rows]
    }



async def get_data_merge(db: AsyncSession, filterCondition: FilterCondition):
    fromDate = filterCondition.fromDate.strftime("%m/%d/%Y")
    toDate = filterCondition.toDate.strftime("%m/%d/%Y")
    selectedCaseTypes = filterCondition.selectedCaseTypes
    selectedCourt = filterCondition.selectedCourt
    selectedCounty = filterCondition.selectedCounty

    filters = [
        Case.CaseStatusDate >= fromDate,
        Case.CaseStatusDate <= toDate,
    ]

    if selectedCaseTypes:
        filters.append(Case.CaseType.in_(selectedCaseTypes))
    if selectedCourt:
        filters.append(Case.Court.in_(selectedCourt))
    if selectedCounty:
        filters.append(Case.DefendantAddressCity.in_(selectedCounty))

    count_stmt = select(func.count()).select_from(Case).filter(*filters)
    result = await db.execute(count_stmt)  # Use await here
    total_count = result.scalar()  # Get the scalar result
    print("total_count:", total_count)
    
    stmt = select(Case).filter(*filters)
    result = await db.execute(stmt)
    rows = result.all()
    columns = result.keys()
    return {"total_count" : total_count, "data" : [dict(zip(columns, row)) for row in rows]}

async def get_last_query_date(db: AsyncSession):
    
    stmt = select(
        Case.CaseStatusDate  # Select the CaseStatusDate
    ).filter(
        Case.CaseStatusDate != None  # Ensure CaseStatusDate is not None
    ).order_by(
        Case.id.desc()  # Order by ID in descending order
    ).limit(1)  # Limit to the latest record

    result = await db.execute(stmt)
    last_date = result.scalar()  # Get the CaseStatusDate for the largest ID

    # Check if last_date is None
    if last_date is None:
        return None  # or handle as appropriate
    else:
        return last_date

async def insert_courts(db: AsyncSession, items: list):
    if len(items) < 2:
        print("No data to insert.")
        return False

    try:
        # Delete all rows using SQLAlchemy's expression API
        await db.execute(delete(Courts))

        # Create Court instances (skip header)
        court_instances = [
            Courts(identifier=item[0], courts=item[1], date=item[2])
            for item in items[1:]
        ]

        db.add_all(court_instances)

        await db.commit()  # One commit for both delete and insert
        print(f"{len(court_instances)} records inserted successfully.")
        return True

    except Exception as e:
        await db.rollback()
        print(f"Error while inserting records: {e}")
        return False
    
async def insert_counties(db: AsyncSession, items: list):
    if len(items) < 2:
        print("No data to insert.")
        return False

    try:
        # Delete all rows using SQLAlchemy's expression API
        await db.execute(delete(Counties))

        # Create Court instances (skip header)
        for idx, item in enumerate(items):
            county = Counties(identifier=str(idx+1).zfill(2), county=item['name'])
            db.add(county)

        await db.commit()  # One commit for both delete and insert
        print(f"county records inserted successfully.")
        return True

    except Exception as e:
        await db.rollback()
        print(f"Error while inserting records: {e}")
        return False
    
async def get_courts(db: AsyncSession):
    stmt = select(Courts)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_paid_courts(db: AsyncSession, username: str):
    # Aliases
    u = User
    co = CourtOwner
    c = Courts

    # Build the query: Join CourtOwner → User and CourtOwner → Courts
    stmt = (
        select(c.courts)
        .join_from(co, u, co.user == u.id)
        .join(c, co.court == c.identifier)
        .where(u.username == username)
    )

    result = await db.execute(stmt)
    return result.scalars().all()

async def get_paid_county(db: AsyncSession, username: str):
    # Aliases
    u = User
    co = CourtOwner
    c = Counties

    # Select distinct counties only
    stmt = (
        select(distinct(c.county))
        .join_from(co, u, co.user == u.id)
        .join(c, co.county == c.identifier)
        .where(u.username == username)
    )

    result = await db.execute(stmt)
    return result.scalars().all()

async def get_indiana_counties(db: AsyncSession):
    stmt = select(Counties)
    result = await db.execute(stmt)
    return result.scalars().all()

async def insert_template(db: AsyncSession, item: TemplateModel):
    
    template = Template(
        origin_name=item.origin_name,
        saved_name=item.saved_name,
        saved_path=item.saved_path,
        template_type=item.template_type,  # or "envelope", depending on the use case
        content=item.content,
        user=item.user
    )
    db.add(template)
    await db.commit()

async def get_templates(db: AsyncSession, username: str):
    
    stmt = select(
        Template.origin_name,
        Template.saved_path,
        Template.template_type,
        Template.content
    ).where(Template.user == username)

    result = await db.execute(stmt)
    rows = result.all()
    
    # Convert tuples to dictionaries
    return [
        {
            "origin_name": row[0],
            "saved_path": row[1],
            "template_type": row[2],
            "content": base64.b64encode(row[3]).decode("utf-8") if row[3] else None
        }
        for row in rows
    ]

async def save_paid_courts(db: AsyncSession, selected_courts: list, user_email: str):
    
    user_stmt = select(User).where(User.username == user_email)
    user_result = await db.execute(user_stmt)
    user = user_result.scalar_one_or_none()

    if not user:
        raise ValueError(f"User with email {user_email} not found")

    court_objects = []

    for court_identifier in selected_courts:
        match = re.match(r"(\d+)", court_identifier)
        countyID = match.group(1)

        court_obj = CourtOwner(
            user=user.id,
            court=court_identifier,
            county=countyID,
            date=datetime.utcnow()
        )
        court_objects.append(court_obj)

    db.add_all(court_objects)
    await db.commit()

async def get_saved_shortcode(db: AsyncSession):
    stmt = select(ShortCodes)
    result = await db.execute(stmt)
    return result.scalars().all()

async def get_fields(db: AsyncSession):
    stmt = select(Fields)
    result = await db.execute(stmt)
    return result.scalars().all()


async def add_new_shortcode(db: AsyncSession, item: ShortcodeModel):
    
    template = ShortCodes(
        field=item.field,
        shortcode=item.shortcode
    )
    db.add(template)
    await db.commit()

async def remove_shortcode(db: AsyncSession, item: ShortcodeModel):
    # Find and delete record(s) matching the given field
    stmt = delete(ShortCodes).where(ShortCodes.field == item.field)
    await db.execute(stmt)
    await db.commit()

async def remove_saved_templates(db: AsyncSession, item: TemplateModel):
    # Find and delete record(s) matching the given field
    stmt = delete(Template).where(Template.origin_name == item.origin_name)
    await db.execute(stmt)
    await db.commit()

async def get_completed_template(db: AsyncSession, data: TemplateCaseModel):
    case_stmt = select(Case).where(Case.id == data.case_id)
    result = await db.execute(case_stmt)
    case = result.scalar_one_or_none()

    if not case:
        return None
    
    case_dict = {col.name.lower(): getattr(case, col.name) for col in case.__table__.columns}
    
    shortcode_stmt = select(ShortCodes)
    result = await db.execute(shortcode_stmt)
    shortcodes = result.scalars().all()

    filled_template = data.template_text

    for shortcode in shortcodes:
        field_key = shortcode.field.lower()
        placeholder = shortcode.shortcode
        value = case_dict.get(field_key, "")
        filled_template = filled_template.replace(placeholder, str(value) if value is not None else "")

    return filled_template


async def get_purchased_courts(db: AsyncSession):
    # Step 1: Fetch all data from tbl_court_owner
    stmt = select(CourtOwner)
    result = await db.execute(stmt)
    court_owners = result.scalars().all()

    if not court_owners:
        return []

    # Step 2: Extract unique court and county identifiers
    court_ids = list(set(co.court for co in court_owners))
    county_ids = list(set(co.county for co in court_owners))

    # Step 3: Fetch courts from tbl_court by identifier
    court_stmt = select(Courts).where(Courts.identifier.in_(court_ids))
    court_result = await db.execute(court_stmt)
    courts = court_result.scalars().all()
    court_map = {c.identifier: c.courts for c in courts}

    # Step 4: Fetch counties from tbl_county by identifier
    county_stmt = select(Counties).where(Counties.identifier.in_(county_ids))
    county_result = await db.execute(county_stmt)
    counties = county_result.scalars().all()
    county_map = {c.identifier: c.county for c in counties}  # id => name

    # Step 5: Group courts under counties by identifier
    grouped = defaultdict(list)
    for co in court_owners:
        county_id = co.county
        court_id = co.court
        grouped[county_id].append({
            "identifier": court_id,
            "courts": court_map.get(court_id, "Unknown Court")
        })

    # Step 6: Build final structure
    result = []
    for county_id, court_list in grouped.items():
        result.append({
            "identifier": county_id,  # county identifier (e.g., "02")
            "name": county_map.get(county_id, "Unknown County"),  # name from tbl_county.county
            "courts": court_list
        })

    return result


async def get_counties_all_data(db: AsyncSession):
    # Step 1: Fetch all counties and courts
    counties_result = await db.execute(select(Counties))
    counties = counties_result.scalars().all()

    courts_result = await db.execute(select(Courts))
    courts = courts_result.scalars().all()

    # Step 2: Group courts by county identifier prefix (first two characters)
    courts_by_county = defaultdict(list)
    for court in courts:
        if court.identifier and len(court.identifier) >= 2:
            prefix = court.identifier[:2]
            courts_by_county[prefix].append({
                "identifier": court.identifier,
                "courts": court.courts
            })

    # Step 3: Assemble final structure
    result = []
    for county in counties:
        result.append({
            "identifier": county.identifier,
            "name": county.county,
            "courts": courts_by_county.get(county.identifier, [])
        })

    return result
