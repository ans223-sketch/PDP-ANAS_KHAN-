import pandas as pd
from multiprocessing import Pool

# Paths to the separate Excel files
students_data_path = 'Students.xlsx'
fees_data_path = 'fees.xlsx'

# Load data
students_data = pd.read_excel(students_data_path)
fees_data = pd.read_excel(fees_data_path)

# Data Preprocessing
fees_data['date_of_fee_submission'] = pd.to_datetime(fees_data['date_of_fee_submission'], errors='coerce')

# Strip and title case the names in students data
students_data['first_name'] = students_data['first_name'].str.strip().str.title()
students_data['last_name'] = students_data['last_name'].str.strip().str.title()

# Function to find the latest submission date for a student
def process_student(student):
    first_name, last_name = student
    # Find the student ID by matching first and last names
    student_id = students_data[(students_data['first_name'] == first_name) & (students_data['last_name'] == last_name)]['StudentID']
    
    if student_id.empty:
        return f"No StudentID found for {first_name} {last_name}."

    student_id = student_id.iloc[0]  # Extract the first StudentID if there are any matches
    # Filter fee data by the matched StudentID
    student_fees_data = fees_data[fees_data['StudentID'] == student_id]

    if student_fees_data.empty:
        return f"No fee submission data found for {first_name} {last_name}."

    # Find the latest fee submission date
    latest_submission_date = student_fees_data['date_of_fee_submission'].max()

    if pd.isnull(latest_submission_date):
        return f"{first_name} {last_name} has no recorded fee submissions."
    
    # Return the formatted output with the latest fee submission date
    return f"{first_name} {last_name}'s latest fee submission date is {latest_submission_date.strftime('%d-%m-%Y')}."

# List of students to process (using first_name and last_name)
students = [("Margaret", "Gonzalez"), ("Justin", "Tyler"), ("Kelly", "Hinton")]

# Parallel Processing
if __name__ == "__main__":
    with Pool(processes=4) as pool:
        results = pool.map(process_student, students)

    # Display results
    for result in results:
        print(result)
