# File for collect methods which check some existence error in db
import mysql
from mysql.connector import connect

# Check movie id already exists
def check_movie_id(cnx, movie_id):
    try:
        query = f"SELECT m_id FROM movie WHERE m_id='{movie_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Check user id already exists
def check_user_id(cnx, user_id):
    try:
        query = f"SELECT c_id FROM user WHERE c_id='{user_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Check user id already booked movie id
def check_reserve_id(cnx, user_id, movie_id):
    try:
        query = f"SELECT m_id, c_id FROM reserve WHERE m_id='{movie_id}' and c_id='{user_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Check user id already rated movie id
def check_rate_id(cnx, user_id, movie_id):
    try:
        query = f"SELECT m_id, c_id, rating FROM reserve WHERE m_id='{movie_id}' and c_id='{user_id}' and rating is NULL"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1
    
# Check user by name and age
def check_user_name_age(cnx, name, age):
    try:
        query = f"SELECT c_name, age FROM user WHERE c_name='{name}' and age='{age}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Check movie by title
def check_movie_title(cnx, title):
    try:
        query = f'SELECT title FROM movie WHERE title="{title}"'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == []:    
                return 0
            else:
                return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1
    
# Check movie is full or not
def check_movie_full(cnx, movie_id):
    try:
        query = f"SELECT c_id FROM reserve WHERE m_id='{movie_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if len(result) >= 10:
                return 1
            else:
                return 0
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1
    
# Check user id rate nothing
def check_user_rate(cnx, user_id):
    try:
        query = f"SELECT m_id FROM reserve WHERE c_id='{user_id}' and rating is not NULL"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result != []:
                return 1
            else:
                return 0
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1