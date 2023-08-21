# File for collect db methods
import mysql
import numpy as np
from mysql.connector import connect

# Create table
def create_table(cnx):
    query1 = '''CREATE TABLE movie (
                    m_id int NOT NULL AUTO_INCREMENT, title varchar(255) NOT NULL, 
                    director varchar(255), price int,
                    PRIMARY KEY (m_id)
                );'''
    query2 = '''CREATE TABLE user (
                    c_id int NOT NULL AUTO_INCREMENT, c_name varchar(255) NOT NULL,
                    age int NOT NULL, class varchar(255),
                    PRIMARY KEY (c_id)
                );'''
    query3 = '''CREATE TABLE reserve (
                    m_id int NOT NULL, c_id int NOT NULL, price int, rating int,
                    PRIMARY KEY (m_id, c_id)
                );'''
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.execute(query3)
            cursor.close()
            cnx.commit()
        return 1
    except mysql.connector.errors.Error:
        return -1

# Drop table
def drop_table(cnx):
    query1 = "DROP TABLE if exists movie;"
    query2 = "DROP TABLE if exists user;"
    query3 = "DROP TABLE if exists reserve;"
    
    # Run quries - delete instance and drop table
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.execute(query3)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.Error:
        return -1
    
# delete all data
def delete_all(cnx):
    query1 = "DELETE FROM movie;"
    query2 = "DELETE FROM user;"
    query3 = "DELETE FROM reserve;"
    
    # Run quries - delete instance and drop table
    try:
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.execute(query3)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.Error:
        return -1
  
# Input user
def input_user(cnx, name, age, user_class):
    try:
        query = f"INSERT INTO user(c_name, age, class) values ('{name}', '{age}', '{user_class}')"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1
    
# Input movie
def input_movie(cnx, title, director, price):
    try:
        query = f'INSERT INTO movie(title, director, price) values ("{title}", "{director}", "{price}")'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.DatabaseError as e:
        print(e)
        return -1
    
# Print all users
def print_all_users(cnx):
    try:
        query = "SELECT c_id, c_name, age, class FROM user ORDER BY c_id"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None

# Print all movies
def print_all_movies(cnx):
    try:
        query = '''SELECT movie.m_id, title, director, movie.price, AVG(reserve.price), COUNT(c_id), AVG(reserve.rating)
                   FROM movie LEFT OUTER JOIN reserve ON movie.m_id = reserve.m_id GROUP BY m_id ORDER BY m_id'''
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
 
# Delete one movie in db   
def delete_movie(cnx, movie_id):
    try:
        query1 = f"DELETE FROM movie WHERE m_id='{movie_id}'"
        query2 = f"DELETE FROM reserve WHERE m_id='{movie_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Delete one user in db
def delete_user(cnx, user_id):
    try:
        query1 = f"DELETE FROM user WHERE c_id='{user_id}'"
        query2 = f"DELETE FROM reserve WHERE c_id='{user_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.close()
            cnx.commit()
            return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

# Reserve movie for user    
def reserve_movie(cnx, movie_id, user_id):
    try:
        query1 = f"SELECT price FROM movie WHERE m_id='{movie_id}'"
        price = 0
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            result = cursor.fetchall()
            price = result[0]['price']
            cursor.close()
        query2 = f"SELECT class FROM user WHERE c_id='{user_id}'"
        dc_rate = 0
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query2)
            result = cursor.fetchall()
            user_class = result[0]['class']
            cursor.close()
        if user_class == 'basic':
            dc_rate = 0
        elif user_class == 'premium':
            dc_rate = 0.25
        elif user_class == 'vip':
            dc_rate = 0.5
        new_price = (1-dc_rate) * price
        query3 = f"INSERT INTO reserve(m_id, c_id, price) values ('{movie_id}', '{user_id}', '{new_price}')"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query3)
            cursor.close()
            cnx.commit()
        return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1
    
# Rate movie for user
def rating_movie(cnx, movie_id, user_id, rating):
    try:
        query = f"UPDATE reserve SET rating='{rating}' WHERE m_id='{movie_id}' and c_id='{user_id}'"
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            cursor.close()
            cnx.commit()
        return 1
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return -1

def print_movie_users(cnx, movie_id):
    try:
        query = f'''SELECT user.c_id, user.c_name, user.age, reserve.price, reserve.rating FROM user INNER JOIN reserve 
                    WHERE user.c_id=reserve.c_id and reserve.m_id="{movie_id}" ORDER BY user.c_id'''
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None

def print_user_movies(cnx, user_id):
    try:
        query = f'''SELECT movie.m_id, title, director, reserve.price, reserve.rating 
            FROM movie, reserve WHERE movie.m_id=reserve.m_id and reserve.c_id="{user_id}"'''
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
# Get user class by id
def get_user_class_by_id(cnx, user_id):
    try:
        query = f'SELECT class FROM user WHERE c_id="{user_id}"'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result[0]
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
# Get user id by name, age
def get_user_id_by_name_age(cnx, name, age):
    try:
        query = f'SELECT c_id FROM user WHERE c_name="{name}" and age="{age}"'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result[0]
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
# Get movie id by title
def get_movie_id_by_title(cnx, title):
    try:
        query = f'SELECT m_id FROM movie WHERE title="{title}"'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result[0]
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None

# Find movie list that user already watch
def get_user_watch_list(cnx, user_id):
    try:
        result = []
        query = f'SELECT m_id FROM reserve WHERE c_id="{user_id}"'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            tmp = cursor.fetchall()
            cursor.close()
            for col in tmp:
                result.append(col['m_id'])
            return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
# Recommend with avg rating
def recommend_by_rating(cnx, user_id):
    try:
        query = f'''SELECT movie.m_id, title, AVG(movie.price), COUNT(reserve.c_id), AVG(reserve.rating) FROM movie, reserve 
        WHERE movie.m_id=reserve.m_id GROUP BY reserve.m_id ORDER BY AVG(reserve.rating) DESC, movie.m_id'''
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == None:
                return None
            already_watch = get_user_watch_list(cnx, user_id)
            for i in range(0, len(result)):
                if result[i]['m_id'] in already_watch:
                    continue
                return result[i]
            return None
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
# Recommend_by_number
def recommend_by_number(cnx, user_id):
    try:
        query = f'''SELECT movie.m_id, title, AVG(movie.price), COUNT(reserve.c_id), AVG(reserve.rating) FROM movie, reserve 
        WHERE movie.m_id=reserve.m_id GROUP BY reserve.m_id ORDER BY COUNT(reserve.c_id) DESC, movie.m_id'''
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            if result == None:
                return None
            already_watch = get_user_watch_list(cnx, user_id)
            for i in range(0, len(result)):
                if result[i]['m_id'] in already_watch:
                    continue
                return result[i]
            return None
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None

# Recommend_by_matrix
def recommend_by_matrix(cnx, user_id, rec_count):
    # Find number of user and movie
    user_num = 0
    movie_num = 0
    try:
        query1 = f'SELECT MAX(c_id) FROM user'
        query2 = f'SELECT MAX(m_id) FROM movie'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query1)
            user_num = cursor.fetchall()[0]['MAX(c_id)']
            cursor.close()
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query2)
            movie_num = cursor.fetchall()[0]['MAX(m_id)']
            cursor.close()
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    
    # Make user item matrix
    user_item = np.zeros((user_num, movie_num))
    user_item[:] = np.nan
    already_watch = get_user_watch_list(cnx, user_id)
    try:
        query3 = f'SELECT * FROM reserve'
        with cnx.cursor(dictionary=True) as cursor:
            cursor.execute(query3)
            result = cursor.fetchall()
            cursor.close()
            for row in result:
                if row['rating'] != None:
                    user_item[row['c_id']-1][row['m_id']-1] = row['rating']
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    column_mean = np.nanmean(user_item, axis=0)
    for i in range(0, user_num):
        for j in range(0, movie_num):
            if np.isnan(user_item[i][j]):
                user_item[i][j] = column_mean[j]

    # Calculate adjusted cosine similarity
    adj_cos = np.zeros((movie_num, movie_num))
    user_id = int(user_id)
    for i in range(0, movie_num):
        adj_cos[i][i] = 1
        for j in range(i+1, movie_num):
            tmp1 = np.transpose(np.copy(user_item))[i] - np.mean(user_item) + 1e-10
            tmp2 = np.transpose(np.copy(user_item))[j] - np.mean(user_item) + 1e-10
            adj_cos[j][i] = adj_cos[i][j] = np.dot(tmp1, tmp2) / (np.sqrt(np.sum(np.square(tmp1))) * np.sqrt(np.sum(np.square(tmp2))))
    adj_cos = np.round(adj_cos, 4)
    
    # Find max similarity items
    result = []
    calc = np.array([0.] * movie_num)
    for i in range(0, movie_num):
        calc[i] = (np.dot(adj_cos[i], user_item[user_id-1]) - user_item[user_id-1][i]) / (np.sum(adj_cos[i]) - 1)
    item_num = 0
    try:
        while item_num < int(rec_count):
            max_idx = np.argmax(calc)
            if calc[max_idx] == -100000:
                break
            if max_idx+1 in already_watch:
                calc[max_idx] = -100000
                continue
            exp_rating = np.round(calc[max_idx], 2)
            query4 = f'''SELECT movie.m_id, title, AVG(reserve.price), AVG(reserve.rating) FROM movie, reserve
                         WHERE movie.m_id=reserve.m_id and movie.m_id="{max_idx+1}" GROUP BY movie.m_id'''
            with cnx.cursor(dictionary=True) as cursor:
                cursor.execute(query4)
                tmp = cursor.fetchall()[0]
                result.append(tmp)
                result[item_num]['exp_rating'] = exp_rating
                cursor.close
            calc[max_idx] = -100000
            item_num += 1
        return result
    except mysql.connector.errors.ProgrammingError as e:
        print(e)
        return None
    