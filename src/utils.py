import re
from datetime import datetime

def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position


def extract_postion(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_classcode(file_content):
    try:
        classcode_match = re.search(r'(Class Code:)\s+(\d+)', file_content)
        classcode = classcode_match.group(2) if classcode_match else None
        return classcode
    
    except Exception as e:
        raise ValueError(f"Error Extracting class node: {str(e)}")

def extract_start_date(file_content):
    try:
        opendate_match = re.search(r'(Open [Dd]ate:)\s+(\d\d-\d\d-\d\d)', file_content)
        start_date = datetime.strptime(opendate_match.group(2), '%m-%d-%y') if opendate_match else None
        return start_date
    
    except Exception as e:
        raise ValueError(f"Error Extracting start date: {str(e)}")

def extract_end_date(file_content):
    try:
        enddate_match = re.search(
            r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s(\d{1,2},\s\d{4})', file_content,
            file_content
            )
        end_date = datetime.strptime(enddate_match.group(2), '%B %d, %Y') if enddate_match else None
        return end_date
    
    except Exception as e:
        raise ValueError(f"Error Extracting end date: {str(e)}")

def extract_salary(file_content):
    try:
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+\$(\d{1,3}(?:,\d{3})+))?'
        salary_match = re.search(salary_pattern, file_content)
        
        if salary_match:
            salary_start = float(salary_match.group(1).replace(',', ''))
            salary_end = float(salary_match.group(4).replace(',', '')) if salary_match.group(4)  \
                else float(salary_match.group(2).replace(',', ''))
        else:
            salary_start, salary_end = None, None
            
        return salary_start, salary_end
    
    except Exception as e:
        raise ValueError(f"Error Extracting salary: {str(e)}")

def extract_requirements(file_content):
    try:
        requirements_match = re.search(r'(REQUIREMENTS?/\s?MINIMUM QUALIFICATIONS?)(.*)(PROCESS NOTES?)',
                                        file_content,
                                        re.DOTALL)
        requirement = requirements_match.group(2).strip() if requirements_match else None
        return requirement
    
    except Exception as e:
        raise ValueError(f"Error Extracting requirements: {str(e)}")
    
def extract_notes(file_content):
    try:
        notes_match = re.search(r'(NOTES?):(.*?)(?=DUTIES)',
                                        file_content,
                                        re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None
        return notes
    
    except Exception as e:
        raise ValueError(f"Error Extracting notes: {str(e)}")

def extract_duties(file_content):
    try:
        duties_match = re.search(r'(DUTIES):(.*?)(REQ[A-Z])',
                                        file_content,
                                        re.DOTALL)
        duties = duties_match.group(2).strip() if duties_match else None
        return duties
    
    except Exception as e:
        raise ValueError(f"Error Extracting duties: {str(e)}")

def extract_selection(file_content):
    try:
        selection_match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+',
                                        file_content,
                                        re.DOTALL)
        selection = [z[0] for z in selection_match] if selection_match else None
        return selection
    
    except Exception as e:
        raise ValueError(f"Error Extracting selection: {str(e)}")

def extract_experience_required(file_content):
    try:
        experience_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)\s(years?)\s(of\sfull(-|\s)time)',
            file_content)
        
        experience = experience_match.group(1) if experience_match else None
        return experience
    
    except Exception as e:
        raise ValueError(f"Error Extracting experience: {str(e)}")

def extract_education_years(file_content):
    try:
        education_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)(\s|-)(years?)\s(college|university)',
            file_content)
        
        education = education_match.group(1) if education_match else None
        return education
    
    except Exception as e:
        raise ValueError(f"Error Extracting education: {str(e)}")

def extract_application_location(file_content):
    try:
        location_match = re.search(
            r'(Applications? will only be accepted on-?line)',
            file_content,
            re.IGNORECASE)
        
        location = 'Online' if location_match else 'Mail or In Person'
        return location
    
    except Exception as e:
        raise ValueError(f"Error Extracting location: {str(e)}")