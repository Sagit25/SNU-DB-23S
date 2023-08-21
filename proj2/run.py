import mysql
import csv
from dbcheck import *
from dbmethod import *
from mysql.connector import connect

cnx = None

# Problem 1 (5 pt.)
def initialize_database():    
    # Create table
    if create_table(cnx) != 1:
        drop_table(cnx)
        create_table(cnx)
    
    # Initialize database - read csv file and insert
    with open('data.csv', 'r') as file:
        csv_data = csv.reader(file)
        for row in csv_data:
            if row[0] == 'title':
                continue
                
            # Insert movie
            for i in range(1):
                try:
                    price = int(row[2])
                    if price < 0 or price > 100000:
                        print('Movie price should be from 0 to 100000')
                        continue
                    if check_movie_title(cnx, row[0]) != 0:
                        continue  
                    if input_movie(cnx, row[0], row[1], price) != 1:
                        continue
                except:
                    continue

            # Insert user
            for i in range(1):
                try:
                    age = int(row[4])
                    if age < 12 or age > 110:
                        print('User age should be from 12 to 110')
                        continue
                    if row[5] not in ['basic', 'premium', 'vip']:
                        print('User class should basic, premium or vip')
                        continue
                    if check_user_name_age(cnx, row[3], age) != 0:
                        continue
                    if input_user(cnx, row[3], age, row[5]) != 1:
                        continue
                except:
                    continue
                
            # Insert reserve
            for i in range(1):
                try:   
                    user_id = get_user_id_by_name_age(cnx, row[3], int(row[4]))['c_id']
                    movie_id = get_movie_id_by_title(cnx, row[0])['m_id']
                    if check_movie_id(cnx, movie_id) != 1:
                        print(f'Movie {movie_id} does not exist')
                        continue
                    if check_user_id(cnx, user_id) != 1:
                        print(f'User {user_id} does not exist')
                        continue
                    if check_reserve_id(cnx, user_id, movie_id) != 0:
                        print(f'User {user_id} already booked movie {movie_id}')
                        continue
                    if check_movie_full(cnx, movie_id) != 0:
                        print(f'Movie {movie_id} has already been fully booked')
                        continue
                    if reserve_movie(cnx, movie_id, user_id) != 1:
                        print(f'Reserve failed!')
                        continue
                except:
                    continue
            
    # Print message
    print('Database successfully initialized')


# Problem 15 (5 pt.)
def reset():
    # question for check delete
    answer = input('Really delete all tables? (y\\n) ')
    if answer.strip().lower() == "n":
        return
    elif answer.strip().lower() != "y":
        while answer.strip().lower() != "n" and answer.strip().lower() != "y":
            answer = input('Wrong reply. Really delete all tables? (y\\n) ')
        if answer.strip().lower() == "n":
            return
    if delete_all(cnx) != 1:
        pass
    if drop_table(cnx) != 1:
        pass
    
    # Initialize database
    initialize_database()


# Problem 2 (3 pt.)
def print_movies():
    # Print all movies
    result = print_all_movies(cnx)
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-')
    print(str.format(' {:<5} {:<70} {:<35} {:<15} {:<15} {:<15} {:<15} ', 'ID', 'TITLE', 'DIRECTOR', 'ORG.PRICE', 'AVG.PRICE', 'RESERVATION', 'AVG.RATING'))
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-')
    for row in result:       
        rating = row["AVG(reserve.rating)"]
        count = row["COUNT(c_id)"]
        price = row["AVG(reserve.price)"]
        if rating == None:
            rating = "None"
        if price == None:
            price = "None"
        else:
            price = float(price)
        print(f' {row["m_id"]:<5} {row["title"]:<70} {row["director"]:<35} {row["price"]:<15} {price:<15} {count:<15} {rating:<15} ')
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-' + '-'*15 + '-')


# Problem 3 (3 pt.)
def print_users():
    # Print all users
    result = print_all_users(cnx)
    print('-' + '-'*5 + '-' + '-' * 20 + '-' + '-'*5 + '-' + '-'*10 + '-')
    print(str.format(' {:<5} {:<20} {:<5} {:<10} ', 'ID', 'NAME', 'AGE', 'CLASS'))
    print('-' + '-'*5 + '-' + '-' * 20 + '-' + '-'*5 + '-' + '-'*10 + '-')
    for row in result:
        print(f' {row["c_id"]:<5} {row["c_name"]:<20} {row["age"]:<5} {row["class"]:<10} ')
    print('-' + '-'*5 + '-' + '-' * 20 + '-' + '-'*5 + '-' + '-'*10 + '-')
    
    
# Problem 4 (3 pt.)
def insert_movie():   
    # Get input: title, director, price
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')
    
    # Check errors
    try:
        price = int(price)
        if price < 0 or price > 100000:
            print('Movie price should be from 0 to 100000')
            return
    except:
        print('Movie price should be from 0 to 100000')
        return
    if check_movie_title(cnx, title) != 0:
        print(f'Movie {title} already exists')
        return  

    # Insert movie
    if input_movie(cnx, title, director, price) != 1:
        return
    print('One movie successfully inserted')


# Problem 6 (4 pt.)
def remove_movie():    
    # Get input: movie id
    movie_id = input('Movie ID: ')
    
    # Check errors
    if check_movie_id(cnx, movie_id) != 1:
        print(f'Movie {movie_id} does not exist')
        return
    
    # Remove movie
    if delete_movie(cnx, movie_id) != 1:
        return
    print('One movie successfully removed')
    

# Problem 5 (3 pt.)
def insert_user():  
    # Get input: name, age, class
    name = input('User name: ')
    age = input('User age: ')
    user_class = input('User class: ')
    
    # Check errors
    try:
        age = int(age)
        if age < 12 or age > 110:
            print('User age should be from 12 to 110')
            return
    except:
        print('User age should be from 12 to 110')
        return
    if user_class not in ['basic', 'premium', 'vip']:
        print('User class should basic, premium or vip')
        return
    if check_user_name_age(cnx, name, age) != 0:
        print(f'User ({name}, {age}) already exists')
        return
    
    # Input user to db
    if input_user(cnx, name, age, user_class) != 1:
        return
    print('One user successfully inserted')


# Problem 7 (4 pt.)
def remove_user():
    # Get input: user id
    user_id = input('User ID: ')
    
    # Check errors
    if check_user_id(cnx, user_id) != 1:
        print(f'Movie {user_id} does not exist')
        return
    
    # Remove user
    if delete_user(cnx, user_id) != 1:
        return
    print('One user successfully removed')


# Problem 8 (5 pt.)
def book_movie():
    # Get input: movie id, user id
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    
    # Check errors
    if check_movie_id(cnx, movie_id) != 1:
        print(f'Movie {movie_id} does not exist')
        return 
    if check_user_id(cnx, user_id) != 1:
        print(f'User {user_id} does not exist')
        return 
    if check_reserve_id(cnx, user_id, movie_id) != 0:
        print(f'User {user_id} already booked movie {movie_id}')
        return
    if check_movie_full(cnx, movie_id) != 0:
        print(f'Movie {movie_id} has already been fully booked')
        return
    
    # Reserve movie
    if reserve_movie(cnx, movie_id, user_id) != 1:
        return
    print('Movie successfully booked')
    

# Problem 9 (5 pt.)
def rate_movie():
    # Get input: movie id, user id, rating
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')
    
    # Check errors
    try:
        rating = int(rating)
        if rating < 1 or rating > 5:
            print(f'Wrong value for a rating')
            return
        if check_movie_id(cnx, movie_id) != 1:
            print(f'Movie {movie_id} does not exist')
            return 
        if check_user_id(cnx, user_id) != 1:
            print(f'User {user_id} does not exist')
            return 
        if check_reserve_id(cnx, user_id, movie_id) != 1:
            print(f'User {user_id} has not booked movie {movie_id} yet')
            return 
        if check_rate_id(cnx, user_id, movie_id) != 1:
            print(f'User {user_id} has already rated movie {movie_id}')
            return 
    except:
        print(f'Wrong value for a rating')
        return

    # Rate movie
    if rating_movie(cnx, movie_id, user_id, rating) != 1:
        return
    print('Movie successfully rated')


# Problem 10 (5 pt.)
def print_users_for_movie():
    # Get input: movie id
    movie_id = input('Movie ID: ')

    # Check errors
    if check_movie_id(cnx, movie_id) != 1:
        print(f'Movie {movie_id} does not exist')
        return

    # Print users
    result = print_movie_users(cnx, movie_id)
    print('-' + '-'*5 + '-' + '-'*20 + '-' + '-'*5 + '-' + '-'*15 + '-' + '-'*10 + '-')
    print(str.format(' {:<5} {:<20} {:<5} {:<15} {:<10} ', 'ID', 'NAME', 'AGE', 'RSV.PRICE', 'RATING'))
    print('-' + '-'*5 + '-' + '-'*20 + '-' + '-'*5 + '-' + '-'*15 + '-' + '-'*10 + '-')
    for row in result:
        rating = row['rating']
        if rating == None:
            rating = 'None'
        print(f' {row["c_id"]:<5} {row["c_name"]:<20} {row["age"]:<5} {row["price"]:<15} {rating:<10} ')
    print('-' + '-'*5 + '-' + '-'*20 + '-' + '-'*5 + '-' + '-'*15 + '-' + '-'*10 + '-')


# Problem 11 (5 pt.)
def print_movies_for_user():
    # Get input: user id
    user_id = input('User ID: ')

    # Check errors
    if check_user_id(cnx, user_id) != 1:
        print(f'User {user_id} does not exist')
        return

    # Print movies
    result = print_user_movies(cnx, user_id)
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    print(str.format(' {:<5} {:<70} {:<35} {:<15} {:<15} ', 'ID', 'TITLE', 'DIRECTOR', 'RSV.PRICE', 'RATING'))
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    for row in result:
        rating = row["rating"]
        if rating == None:
            rating = 'None'
        print(f' {row["m_id"]:<5} {row["title"]:<70} {row["director"]:<35} {row["price"]:<15} {rating:<15} ')
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    
    
# Problem 12 (6 pt.)
def recommend_popularity():
    # Get input: user id
    user_id = input('User ID: ')
    
    # Check errors
    if check_user_id(cnx, user_id) != 1:
        print(f'User {user_id} does not exist')
        return
    
    # Find maximum rating
    result1 = recommend_by_rating(cnx, user_id)
    
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    print("Rating-based")
    print(str.format(' {:<5} {:<70} {:<15} {:<15} {:<15} ', 'ID', 'TITLE', 'RSV.PRICE', 'RESERVATION', 'AVG.RATING'))
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    if result1 != None:
        rating = result1["AVG(reserve.rating)"]
        price = result1["AVG(movie.price)"]
        if rating == None:
            rating = 'None'
        user_class = get_user_class_by_id(cnx, user_id)['class']
        if user_class == "vip":
            price = float(price) * 0.5
        if user_class == "premium":
            price = float(price) * 0.75
        print(f' {result1["m_id"]:<5} {result1["title"]:<70} {price:<15} {result1["COUNT(reserve.c_id)"]:<15} {rating:<15} ')
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    
    # Find maximum watcher
    result2 = recommend_by_number(cnx, user_id)
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    print("Popularity-based")
    print(str.format(' {:<5} {:<70} {:<15} {:<15} {:<15} ', 'ID', 'TITLE', 'RSV.PRICE', 'RESERVATION', 'AVG.RATING'))
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    if result2 != None:
        rating = result2["AVG(reserve.rating)"]
        price = result2["AVG(movie.price)"]
        if rating == None:
            rating = 'None'
        user_class = get_user_class_by_id(cnx, user_id)['class']
        if user_class == "vip":
            price = float(price) * 0.5
        if user_class == "premium":
            price = float(price) * 0.75
        print(f' {result2["m_id"]:<5} {result2["title"]:<70} {price:<15} {result2["COUNT(reserve.c_id)"]:<15} {rating:<15} ')
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')


# Problem 13 (10 pt.)
def recommend_item_based():
    user_id = input('User ID: ')
    rec_count = input('Recommend Count: ')

    # Check errors
    if check_user_id(cnx, user_id) != 1:
        print(f'User {user_id} does not exist')
        return
    if check_user_rate(cnx, user_id) != 1:
        print('Rating does not exist')
        return
    
    # Find recommend lists
    result = recommend_by_matrix(cnx, user_id, rec_count)
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    print(str.format(' {:<5} {:<70} {:<15} {:<15} {:<15} ', 'ID', 'TITLE', 'RSV.PRICE', 'AVG.RATING', 'EXP.RATING'))
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')
    if result != None:
        for row in result:
            rating = row["AVG(reserve.rating)"]
            price = row["AVG(reserve.price)"]
            exp_rating = row["exp_rating"]
            if rating == None:
                rating = 'None'
            user_class = get_user_class_by_id(cnx, user_id)
            if user_class == "vip":
                price = price * 0.5
            if user_class == "premium":
                price = price * 0.75
            print(f' {row["m_id"]:<5} {row["title"]:<70} {price:<15} {rating:<15} {exp_rating:<15}')
    print('-' + '-'*5 + '-' + '-'*70 + '-' + '-'*35 + '-' + '-'*15 + '-' + '-'*15 + '-')


# Total of 60 pt.
def main():    
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies rated by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using item-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    # Connect to MySQL
    cnx = connect(host="astronaut.snu.ac.kr", port=7000, user="DB2021_15738",
        password="DB2021_15738", db="DB2021_15738", charset="utf8")
    main()
    cnx.close()
    