import json
import re
from datetime import datetime, time
from tracemalloc import start
from aiohttp import Payload
import threading
import subprocess
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.api.types import CategoricalDtype
from sqlalchemy import create_engine

pd.options.mode.chained_assignment = None


loginUrl = ('https://nhspa.sentral.com.au/portal2/user')
secureUrl = ('https://nhspa.sentral.com.au/portal/')
timetableUrl = ('https://nhspa.sentral.com.au/portal/timetable/mytimetable')
academicResultsUrl = ('https://nhspa.sentral.com.au/portal/reports/results/')
reportCommentsUrl = ('https://nhspa.sentral.com.au/portal/reports/comments/')

def getWebsite(session, url=None):
    s = session
    r = s.get(url)
    timestamp = datetime.now()

    return BeautifulSoup(r.content, 'html.parser'), timestamp

def getTable(session, url=None):
    data = getWebsite(session, url)
    timestamp = data[1]
    try:
        table = data[0].body.table
    except:
        return None, timestamp
    return table, timestamp


def getTables(session, url=None):
    data = getWebsite(session, url)
    table = data[0].find_all('table')
    timestamp = data[1]
    return table, timestamp


def export_payload(payload):
    with open('loginpayload.json', 'w') as file:
        json.dump(payload, file)


def load_payload():
    with open('loginpayload.json', 'r') as file:
        return json.load(file)


def set_payload(username=None, password=None):
    if not username or not password:
        username = input('Username: ')
        password = input('Password: ')
    return {
        "action": "login",
        "username": username,
        "password": password,
    }

def getPeriodTimes(Day, Period):
    Normal_period_times = {'0': [time(7, 30), time(8, 45)], '1': [time(8, 55), time(10, 17)],
                           '2': [time(10, 37), time(11, 54)], '3': [time(11, 56), time(13, 13)],
                           '4': [time(13, 53), time(15, 10)], '5': [time(15, 10), time(16, 30)]}
    Thursday_period_times = {'0': [time(7, 30), time(8, 45)], '1': [time(8, 55), time(10, 17)],
                             '2': [time(10, 17), time(10, 47)], '3': [time(11, 7), time(12, 24)],
                             '4': [time(1, 4), time(14, 21)], '5': [time(14, 30), time(16, 0)]}
    Thursday_wet_weather_times = {'0': [time(7, 30), time(8, 45)], '1': [time(8, 55), time(10, 25)],
                                  '3': [time(10, 45), time(12, 10)], '4': [time(12, 56), time(2, 21)],
                                  '5': [time(14, 30), time(16, 0)]}

    try:
        if Day == "Thursday":
            start = Thursday_period_times[Period][0]
            end = Thursday_period_times[Period][1]
        else:
            start = Normal_period_times[Period][0]
            end = Normal_period_times[Period][1]
    except:
        return time(8,55),time(15,10)
    return start, end


class Account:
    def __init__(self, username=None, password=None, login_payload=None):
        self.session = requests.session()
        if not login_payload:
            login_payload = {
                "action": "login",
                "username": username,
                "password": password,
            }
        self.daily_timetable_Columns = ['Date', 'Week', 'Day', 'Period', 'Subject', 'Class', 'Teacher', 'Room', 'Year',
                                        'Start', 'End', 'Timestamp']

        self.timetable_Columns = ['Day', 'Week', 'Period', 'Subject',
                                  'Class', 'Teacher', 'Room', 'Year']

        r = self.session.post(loginUrl, data=login_payload)
        try:
            self.meta = json.loads(r.content)
        except:
            self.meta = {'Time': np.datetime64('now')}
        if 'error' in self.meta:
            self.meta = pd.DataFrame(
                data=self.meta, columns=list(self.meta), index=[1])
        else:
            try:
                meta = pd.read_json(r.content)
                self.meta = {
                    'First_Name': meta['first_name'][0],
                    'Last_Name': meta['last_name'][0],
                    'Student_id': meta['student_id'][0],
                    'Email': meta['email'][0],
                }

            except:
                pass

    def get_timetable(self):
        timetable, timestamp = getTable(self.session, timetableUrl)
        if not timetable:
            return 'Timetable not found'
        timetable = pd.read_html(str(timetable))
        df = pd.DataFrame(timetable[0])

        days = {'MonA': 'Monday', 'TueA': 'Tuesday', 'WedA': 'Wednesday', 'ThuA': 'Thursday',
                'FriA': 'Friday', 'MonB': 'Monday', 'TueB': 'Tuesday', 'WedB': 'Wednesday',
                'ThuB': 'Thursday', 'FriB': 'Friday'}

        shape = df.shape
        Database = pd.DataFrame(columns=self.timetable_Columns)

        for x in range(shape[1]):
            for y in range(shape[0]):
                if str(df[x][y]) != 'nan':
                    if len(df[x][y].split(' ')) != 1:
                        # locate time
                        if y < 11:
                            day = df[x][0]
                            Week = 'A'
                        else:
                            day = df[x][11]
                            Week = 'B'
                        if 'Thu' not in df[x][0]:
                            period = df[0][y]
                        else:
                            period = df[4][y]

                        data = pd.DataFrame(
                            parser(df[x][y], days[day], period, Week))
                        Database = Database.append(data, ignore_index=True)
        day_order = CategoricalDtype(
            ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
            ordered=True
        )
        week_order = CategoricalDtype(['A', 'B'], ordered=True)
        Database['Week'] = Database['Week'].astype(week_order)
        Database['Day'] = Database['Day'].astype(day_order)
        Database = Database.sort_values(['Week', 'Day'])
        Database['Timestamp'] = timestamp
        return Database

    def get_daily_timetable(self, rotation=-1):
        daily_timetable, timestamp = getTable(
            self.session, timetableUrl + '/' + str(rotation) + '/daily')
        if not daily_timetable:
            return False
        daily_timetable = pd.read_html(str(daily_timetable))
        df = pd.DataFrame(daily_timetable[0])

        shape = df.shape

        Database = pd.DataFrame(columns=self.daily_timetable_Columns)
        for x in range(shape[1]):
            for y in range(shape[0]):
                if str(df[x][y]) != 'nan':
                    if str(df[x][y]) == 'Holiday':
                        if y == 0:
                            day = df[x][1]
                            date = df[x][2]
                            Week = 'A'
                        else:
                            Week = 'B'
                            day = df[x][14]
                            date = df[x][15]
                        data = {'Subject': 'Holiday',
                                'Period': 'All-Day', 'Day': day, 'Date': date}
                    elif len(df[x][y].split(' ')) != 1:
                        # locate time
                        if y < 13:
                            day = df[x][1]
                            date = df[x][2]
                            Week = 'A'
                        else:
                            day = df[x][14]
                            date = df[x][15]
                            Week = 'B'
                        if 'Thu' not in df[x][0]:
                            period = df[0][y]
                        else:
                            period = df[4][y]
                        data = pd.DataFrame(
                            parser(df[x][y], day, period, Week, date))
                        Database = Database.append(data, ignore_index=True)
        Database['Date'] = Database['Date'].astype('datetime64')
        Database['Timestamp'] = timestamp
        Database = Database.sort_values('Date', ignore_index=True)
        return Database

    def get_daily_classes_dataset(self, start=1):
        rotation = start
        Database = pd.DataFrame(columns=self.daily_timetable_Columns)
        NewData = pd.DataFrame()
        Defualt = self.get_daily_timetable(-1)
        DefaultDate = Defualt['Date']
        Duplicates = False
        while Duplicates < 2:
            NewData = self.get_daily_timetable(rotation)

            if NewData['Date'].equals(DefaultDate):
                Duplicates += 1
            if Duplicates < 2:
                Database = Database.append(NewData, ignore_index=True)
                rotation += 1

        Database.sort_values(by='Start', inplace=True)
        return Database

    def update_daily_classes(self, forwards=1, backwards=0):
        tmp = getWebsite(self.session, timetableUrl)[0]
        tmp = tmp.find('a', {'class': 'btn btn-success'}).get('href')
        currentRotation = [int(i) for i in re.findall(r'-?\d+\.?\d*', tmp)][0]
        return self.get_daily_classes_range(currentRotation-backwards, currentRotation+forwards+1)

    def get_daily_classes_range(self, start=None, end=None):
        Database = pd.DataFrame(columns=self.daily_timetable_Columns)
        NewData = pd.DataFrame()
        Defualt = self.get_daily_timetable(-1)
        duplicates = 0
        for rotation in range(start, end):
            NewData = self.get_daily_timetable(rotation)
            try:
                if NewData['Date'][0] == Defualt['Date'][0]:
                    if NewData['Date'][0] < OldData['Date'][0]:
                        break
                    duplicates += 1
                    if duplicates == 2:
                        break
            except:
                pass
            Database = Database.append(NewData, ignore_index=True)
            OldData = NewData

        Database.sort_values(by='Start', inplace=True)
        return Database
    def parser(data, Day, Period, Week, date=None, Rotation=None):
        data = data.replace('*', '')
        if 'Study' not in data and 'Orchestra' not in data:
            [Subject, data] = data.split(' Yr')
            [Year, data] = data.split(' (')
            Year = Year.replace(' ', '')
            [Class, data] = data.split(')  ')
            try:
                [Room, Teacher] = data.split('  with  ')
            except:
                Teacher = 'N/a'
                Room = data.replace(' with', '')
            Room = Room.replace('Room ', '')
            # Teacher = Teacher.removesuffix('.')
            Teacher = Teacher.replace('.', '')
        elif 'Symphony Orchestra' in data:
            Subject = 'Symphony Orchestra'
            Class = 'Orchestra'
            Year = 'N/a'
            Room = 'N/a'
            Teacher = 'N/a'
        elif 'Jazz Orchestra' in data:
            Subject = 'Jazz Orchestra'
            Class = 'Jazz'
            Year = 'N/a'
            Room = 'N/a'
            Teacher = 'N/a'
        else:
            Subject = 'Study'
            Class = data.replace('Study ', '')
            Class = Class.replace(')  with  Study', '')
            Class = Class.replace(' ', '')
            Class = Class.replace('(', '')
            Year = 'N/a'
            Room = 'N/a'
            Teacher = 'N/a'
        data = {'Day': [Day], 'Week': [Week], 'Period': [Period], 'Subject': [Subject], 'Class': [Class],
                'Teacher': [Teacher], 'Room': [Room], 'Year': [Year]}
        start, end = getPeriodTimes(Day, Period)
        if date:
            split_date = date.split('/')
            date = (split_date[1] + '/' + split_date[0] + '/' + split_date[2])
            data['Date'] = date
            start = datetime.combine(datetime.strptime(date, "%m/%d/%Y"), start)
            end = datetime.combine(datetime.strptime(date, "%m/%d/%Y"), end)
        data['Start'] = start
        data['End'] = end

        return data

    def get_reports(self):
        AcademicResults, timestamp = getWebsite(
            self.session, academicResultsUrl)
        AcademicResults = (AcademicResults.find(
            'ul', {'class': 'position-top-right year-selector'}).ul)
        Reports = []
        for link in AcademicResults.findAll('a'):
            Reports.append(self.get_report(link.get('href')))
        Reports = pd.concat(Reports, ignore_index=True, sort=False)
        return Reports

    def get_report(self, url=None):
        acedemicResultsTable, timestamp = getTables(
            self.session, 'https://nhspa.sentral.com.au' + url)
        commentsTable, timestamp = getTables(
            self.session, ('https://nhspa.sentral.com.au' + url).replace('results', 'comments'))
        Reports = []
        for x in range(len(acedemicResultsTable)):
            acedemicResults = pd.read_html(str(acedemicResultsTable[x]))
            acedemicResults = pd.DataFrame(acedemicResults[0])
            comments = pd.read_html(str(commentsTable[x]))[0]
            Reports.append(extract_report(
                acedemicResults, comments, timestamp))
        Report = pd.concat(Reports, ignore_index=True, sort=False)

        return Report


def extract_report(acedemicResults, comments, timestamp):
    tmp = acedemicResults.columns[0][0].replace(',', '')
    tmp = tmp.replace('- ', '')
    tmp = tmp.replace('Year ', '')
    tmp = tmp.replace('Semester ', '')
    tmp = tmp.replace('Stage ', '')
    stage = None
    [semester, year, grade] = tmp.split(' ')
    if len(grade) == 4:
        grade, year = year, grade
    if 'Stage' in acedemicResults.columns[0][0]:
        stage, grade = grade, None
    acedemicResults = acedemicResults[acedemicResults.columns[0][0]]

    bufferDict = {}

    for i in range(int((comments.shape[0] - 1) / 2)):
        bufferDict[comments[0][i * 2]] = comments[0][i * 2 + 1]
    comments = []

    for i in range(acedemicResults.shape[0]):
        comments.append(bufferDict[acedemicResults['Subject'][i]])
    acedemicResults['Comment'] = comments

    acedemicResults['Year'] = year
    acedemicResults['Semester'] = semester
    acedemicResults['Grade'] = grade
    acedemicResults['Stage'] = stage
    acedemicResults['Timestamp'] = timestamp

    return acedemicResults




def unitTest():
    account = Account(None,None,load_payload())
    print(account.get_daily_classes_dataset())


if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    unitTest()
