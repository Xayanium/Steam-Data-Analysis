import random
from faker import Faker

fake = Faker()

# 预定义游戏元数据
GENRES = ['Action', 'RPG', 'Adventure', 'Strategy', 'FPS', 
         'Simulation', 'Sports', 'Horror', 'Puzzle', 'Open World']
DEVELOPERS = ['Valve', 'Ubisoft Montreal', 'CD Projekt Red', 'Bethesda Game Studios',
             'Rockstar North', 'Mojang Studios', 'FromSoftware', 'Naughty Dog']
PUBLISHERS = ['Valve', 'Ubisoft', 'CD Projekt', 'Bethesda Softworks',
             'Rockstar Games', 'Mojang', 'Bandai Namco', 'Sony Interactive']

def generate_games_sql(file_path, num_entries=150):
    with open(file_path, 'w') as f:
        f.write("USE steam_data;\n\n")
        for game_id in range(1, num_entries + 1):
            title = fake.catch_phrase().replace("'", "''")  # 处理单引号
            developer = random.choice(DEVELOPERS)
            publisher = random.choice(PUBLISHERS)
            release_date = fake.date_between('-10y', 'today').strftime('%Y-%m-%d')
            price = round(random.uniform(5.99, 89.99), 2)
            genres = ', '.join(random.sample(GENRES, k=random.randint(1, 3)))
            rating = random.randint(50, 100)
            description = fake.sentence(nb_words=10).replace("'", "''")
            
            f.write(
                f"INSERT INTO games VALUES ("
                f"{game_id}, "
                f"'{title}', "
                f"'{developer}', "
                f"'{publisher}', "
                f"'{release_date}', "
                f"{price}, "
                f"'{genres}', "
                f"{rating}, "
                f"'{description}'"
                f");\n"
            )

if __name__ == "__main__":
    generate_games_sql("games.sql")