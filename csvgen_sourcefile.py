import csv
##for enabling logging
import google.cloud.logging
client = google.cloud.logging.Client()
client.setup_logging()

##csv file generation
from faker import Faker
fake=Faker()

##creating a csv file in working folder and writing to it
"""with open('csvgen_sourcefile1.csv', 'w',newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_MINIMAL)
    filewriter.writerow(['First-name',' Last-name','Country','email','married?','Salary','job','fav_col','age','datetime'])
    for _ in range(10000):
        filewriter.writerow([fake.first_name(),fake.last_name(),fake.country(),fake.ascii_free_email(),fake.pybool(),fake.pyint(),fake.job(),fake.safe_color_name(),fake.pyint(),fake.date()])
"""

###appending more 5000 rows to csv file in working folder
with open('csvgen_sourcefile1.csv','a',newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_MINIMAL)
    for _ in range(5000):
        filewriter.writerow([fake.first_name(),fake.last_name(),fake.country(),fake.ascii_free_email(),fake.pybool(),fake.pyint(),fake.job(),fake.safe_color_name(),fake.pyint(),fake.date()])


