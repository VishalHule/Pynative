import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import datetime
from dateutil.relativedelta import relativedelta

def getDbConnection():
    #Get Database connection
    try:
        connection = mysql.connector.connect(host='localhost',
                             database='python_db',
                             user='pynative',
                             password='pynative@#29')

        return connection
    except mysql.connector.Error as error :
        print("Failed to connect to database {}".format(error))

def closeDbConnection(connection):
    #Close Database connection
    try:
        connection.close()
    except mysql.connector.Error as error :
        print("Failed to close database connection {}".format(error))

def readAllHospitalsData():
    #Read data from Hospital table
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select * from Hospital"""
        cursor.execute(sql_select_query)
        records = cursor.fetchall()

        print("Printing all Hospitals record")
        for row in records:
            print("Hospital Id: = ", row[0], )
            print("Hospital Name: = ", row[1])
            print("Bed Count:  = ", row[2], "\n")

        closeDbConnection(connection)
    except mysql.connector.Error as error :
        print("Failed to read hospital and doctor's data {}".format(error))

def readAllDoctorsData():
    # Read data from Doctor table
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select * from Doctor"""
        cursor.execute(sql_select_query)
        records = cursor.fetchall()

        print("Printing all Doctors record")
        for row in records:
            print("Doctor Id: = ", row[0])
            print("Doctor Name: = ", row[1])
            print("Hospital Id: = ", row[2])
            print("Joining Date: = ", row[3])
            print("Speciality: = ", row[4])
            print("Salary: = ", row[5])
            print("Experience:  = ", row[6], "\n")

        closeDbConnection(connection)
    except mysql.connector.Error as error :
        print("Failed to read hospitals data {}".format(error))

def getSpecialistDoctorsList(Speciality, Salary):
    #Fetch doctor's details as per Speciality
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select * from Doctor where Speciality=%s and Salary > %s"""
        cursor.execute(sql_select_query, (Speciality, Salary))
        records = cursor.fetchall()

        print("Printing Doctors record as per given Speciality")
        for row in records:
            print("Doctor Id: = ", row[0])
            print("Doctor Name: = ", row[1])
            print("Hospital Id: = ", row[2])
            print("Joining Date: = ", row[3])
            print("Speciality: = ", row[4])
            print("Salary: = ", row[5])
            print("Experience:  = ", row[6], "\n")

        closeDbConnection(connection)

    except mysql.connector.Error as error :
        print("Failed to read doctor's data {}".format(error))

def getHospitalName(HospitalId):
    #Fetch Hospital Name using Hospital Id
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select * from Hospital where Hospital_Id = %s"""
        cursor.execute(sql_select_query, (HospitalId, ))
        record = cursor.fetchone()
        closeDbConnection(connection)
        return record[1]
    except mysql.connector.Error as error :
        print("Failed to read hospital data {}".format(error))

def GetDoctordWithinHospital(hospitalId):
    #Fetch All doctors within given Hospital
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select * from Doctor where Hospital_Id = %s"""
        cursor.execute(sql_select_query, (hospitalId, ))
        records = cursor.fetchall()

        print("Printing Doctors Within given Hospital")
        for row in records:
            print("Doctor Id: = ", row[0])
            print("Doctor Name: = ", row[1])
            print("Hospital Id: = ", row[2])
            hospitalName = getHospitalName(row[2])
            print("Hospital Name: = ", hospitalName)
            print("Joining Date: = ", row[3])
            print("Speciality: = ", row[4])
            print("Salary: = ", row[5])
            print("Experience:  = ", row[6], "\n")
        closeDbConnection(connection)
    except mysql.connector.Error as error :
        print("Failed to read doctor's data within Hospital {}".format(error))

def getDoctorJoiningDate(DoctorId):
    #Get Doctor's joning date using doctor ID
    try:
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """select Joining_Date from Doctor where Doctor_Id = %s"""
        cursor.execute(sql_select_query, (DoctorId, ))
        joinDate = cursor.fetchone()
        closeDbConnection(connection)
        return joinDate
    except mysql.connector.Error as error :
        print("Failed to read hospital data {}".format(error))

def updateDoctorsExperience(doctorId):
    #Update Doctor Experience in Years
    try:
        #calculate Experience in years
        joningDate = getDoctorJoiningDate(doctorId)
        joningDate = datetime.datetime.strptime(''.join(map(str, joningDate)), '%Y-%m-%d')
        todays_Date = datetime.datetime.now()
        Experience_in_years = relativedelta(todays_Date, joningDate).years

        #Update doctor's Experience now
        connection = getDbConnection()
        cursor = connection.cursor()
        sql_select_query = """update Doctor set Experience = %s where Doctor_Id =%s"""
        cursor.execute(sql_select_query, (Experience_in_years, doctorId))
        connection.commit()
        print(doctorId, "Doctor experience updated to ", Experience_in_years," years")
        closeDbConnection(connection)

    except mysql.connector.Error as error :
        connection.rollback()
        print("Failed to read doctor's data {}".format(error))

print("Start of a Python Database Programming Exercise\n\n")

readAllHospitalsData()
readAllDoctorsData()
getSpecialistDoctorsList("Garnacologist", 30000)
GetDoctordWithinHospital(2)
updateDoctorsExperience(101)

print("End of a Python Database Programming Exercise\n\n")
