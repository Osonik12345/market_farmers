import psycopg2
from psycopg2.extras import RealDictCursor
import math

print("🔐 Настройка подключения к PostgreSQL")
db_user = input("Введите имя пользователя: ").strip()
db_password = input("Введите пароль: ").strip()
db_name = input("Введите имя БД (по умолчанию itmo_farmers): ").strip() or "itmo_farmers"

DB_CONFIG = {
    "host": "localhost",
    "database": db_name,
    "user": db_user,
    "password": db_password
}

# === Настройки ===
user_input_digit = "\nВведите цифру для выбора пункта меню: "

# === Подключение к БД ===
def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"❌ Не удалось подключиться к БД: {e}")
        return None

# === Пагинация ===
def paginate(items, page=1, per_page=10):
    total = len(items)
    pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], page, pages

# === 1. Просмотр всех рынков (с пагинацией) ===
def farm_list_function():
    conn = get_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fm.market_name, fm.city, fm.state, COALESCE(AVG(r.rating), 0) AS avg_rating,
                       COUNT(r.review_id) AS review_count
                FROM farmers_markets fm
                LEFT JOIN reviews r ON fm.market_id = r.market_id
                GROUP BY fm.market_id, fm.market_name, fm.city, fm.state
                ORDER BY fm.market_name
            """)
            markets = cur.fetchall()

        if not markets:
            print("\n❌ Нет данных о рынках.")
            return

        page = 1
        while True:
            page_items, current_page, total_pages = paginate(markets, page, 10)
            print(f"\n--- Страница {current_page} из {total_pages} ---")
            for m in page_items:
                avg = m['avg_rating']
                stars = "★" * int(round(avg)) + "☆" * (5 - int(round(avg)))
                print(f"📍 {m['market_name']} | {m['city']}, {m['state']} | {stars} ({avg:.1f}) [{m['review_count']} отзывов]")

            print(f"\nСтраница {current_page}/{total_pages}")
            nav = input("\n[П]редыдущая, [С]ледующая, [0] Назад: ").strip().lower()
            if nav == '0':
                break
            elif nav in ['п', 'назад', 'prev', 'p'] and page > 1:
                page -= 1
            elif nav in ['с', 'вперёд', 'next', 'n'] and page < total_pages:
                page += 1
    except Exception as e:
        print(f"❌ Ошибка при загрузке списка: {e}")
    finally:
        conn.close()

def show_menu(menu_dict):
    print("\n" + "-" * 40)
    for key, value in menu_dict.items():
        print(f"{key}. {value[1]}")
    print("-" * 40)
    try:
        return int(input(user_input_digit))
    except ValueError:
        return -1

def find_in_menu(menu_dict, user_choice):
    if user_choice == 0:
        return True, "\nШаг назад"
    elif user_choice in menu_dict:
        return False, menu_dict[user_choice][0]
    else:
        return False, "\nОшибка ввода. Повтори ввод!\n"

# === 4. Оставить отзыв ===
def input_feedback_function():
    market_name = input("Введите название рынка: ").strip()
    user_name = input("Введите ваше имя: ").strip()
    try:
        rating = int(input("Рейтинг (1-5): "))
        if rating < 1 or rating > 5:
            print("❌ Рейтинг должен быть от 1 до 5.")
            return
    except ValueError:
        print("❌ Введите число от 1 до 5.")
        return

    review_text = input("Текст отзыва (можно оставить пустым): ").strip()
    review_text = review_text or None

    conn = get_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT market_id FROM farmers_markets WHERE LOWER(market_name) = %s", (market_name.lower(),))
            market = cur.fetchone()
            if not market:
                print("❌ Рынок не найден.")
                return

            cur.execute("""
                INSERT INTO reviews (market_id, user_name, rating, review_text)
                VALUES (%s, %s, %s, %s)
            """, (market['market_id'], user_name, rating, review_text))
            conn.commit()
            print("✅ Отзыв успешно добавлен!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
    finally:
        conn.close()

# === 3. Подробная информация о рынке ===
def farm_detail_information(market_name=None):
    if not market_name:
        market_name = input("Введите название рынка: ").strip()
    if not market_name:
        print("❌ Пустое имя.")
        return

    conn = get_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM farmers_markets WHERE LOWER(market_name) = %s", (market_name.lower(),))
            market = cur.fetchone()

            if not market:
                print("❌ Рынок не найден.")
                return

            print(f"\n--- {market['market_name']} ---")
            print(f"📍 Адрес: {market['street']}, {market['city']}, {market['state']} {market['zip']}")
            print(f"🌐 Координаты: ({market['x']}, {market['y']})")
            if market['location'] and str(market['location']).strip() != '':
                print(f"📍 Местоположение: {market['location']}")

            # Продукты
            cur.execute("""
                SELECT p.product_name
                FROM market_products mp
                JOIN products p ON mp.product_id = p.product_id
                WHERE mp.market_id = %s
                ORDER BY p.product_name
            """, (market['market_id'],))
            products = cur.fetchall()
            if products:
                print("\n🍎 Продукты:")
                for p in products:
                    print(f" • {p['product_name']}")

            # Оплата
            cur.execute("""
                SELECT py.payment_name
                FROM market_payments mp
                JOIN payment_methods py ON mp.payment_id = py.payment_id
                WHERE mp.market_id = %s
                ORDER BY py.payment_name
            """, (market['market_id'],))
            payments = cur.fetchall()
            if payments:
                print("\n💳 Способы оплаты:")
                for p in payments:
                    print(f" • {p['payment_name']}")

            # Соцсети
            cur.execute("""
                SELECT sn.social_networks, msl.url
                FROM market_social_links msl
                JOIN social_networks sn ON msl.social_network_id = sn.social_network_id
                WHERE msl.market_id = %s
                ORDER BY sn.social_networks
            """, (market['market_id'],))
            socials = cur.fetchall()
            if socials:
                print("\n🌐 Социальные сети:")
                for s in socials:
                    url = s['url'] if s['url'] else "нет ссылки"
                    print(f" • {s['social_networks']}: {url}")

            # Отзывы
            cur.execute("""
                SELECT user_name, rating, review_text, created_at
                FROM reviews
                WHERE market_id = %s
                ORDER BY created_at DESC
            """, (market['market_id'],))
            reviews = cur.fetchall()
            if reviews:
                print(f"\n💬 Отзывы ({len(reviews)}):")
                for r in reviews:
                    stars = "★" * r['rating'] + "☆" * (5 - r['rating'])
                    date_str = r['created_at'].strftime('%d.%m.%Y')
                    print(f"[{r['user_name']}] {stars} ({date_str})")
                    if r['review_text']:
                        print(f"   \"{r['review_text']}\"")
            else:
                print("\n📝 Отзывов пока нет.")

    except Exception as e:
        print(f"❌ Ошибка при получении данных: {e}")
    finally:
        conn.close()

# === 5. Удалить рынок ===
def delete_farm_function():
    market_name = input("Введите название рынка для удаления: ").strip()
    confirm = input(f"Удалить рынок '{market_name}'? (да/нет): ").strip().lower()
    if confirm not in ['да', 'yes', 'y']:
        print("❌ Удаление отменено.")
        return

    conn = get_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM farmers_markets WHERE LOWER(market_name) = %s RETURNING market_id", (market_name.lower(),))
            if cur.fetchone():
                conn.commit()
                print(f"✅ Рынок '{market_name}' удалён.")
            else:
                print("❌ Рынок не найден.")
    except Exception as e:
        print(f"❌ Ошибка при удалении: {e}")
        conn.rollback()
    finally:
        conn.close()

# === Haversine formula — расстояние между точками (в милях) ===
def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # радиус Земли в милях
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# === 2. Поиск рынков ===
def search_farm_function():
    search_menu = {
        1: ("city", "Поиск по городу"),
        2: ("state", "Поиск по штату"),
        3: ("zip", "Поиск по почтовому индексу"),
        4: ("radius", "Поиск по радиусу (введите координаты)"),
        0: (None, "Вернуться в главное меню")
    }

    while True:
        choice = show_menu(search_menu)
        flag, data = find_in_menu(search_menu, choice)
        if flag:
            print(data)
            return
        search_type = data

        if search_type == "radius":
            try:
                lat = float(input("Введите широту (y): "))
                lon = float(input("Введите долготу (x): "))
                radius = float(input("Введите радиус в милях: "))
            except ValueError:
                print("❌ Неверный формат числа.")
                continue
        else:
            query_value = input(f"Введите {search_type}: ").strip()
            if not query_value:
                print("❌ Пустой ввод.")
                continue

        # Сортировка
        print("""
Выберите сортировку:
1. По возрастанию названия
2. По убыванию названия
3. По рейтингу (по убыванию)
0. Без сортировки
        """)
        try:
            sort_choice = int(input(user_input_digit))
        except ValueError:
            sort_choice = -1

        if sort_choice == 0:
            sort_mode = 0
        elif sort_choice == 1:
            sort_mode = 1
        elif sort_choice == 2:
            sort_mode = 2
        elif sort_choice == 3:
            sort_mode = 3
        else:
            print("❌ Неверный выбор, сортировка отключена.")
            sort_mode = 0

        # Выполнение поиска
        conn = get_connection()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                if search_type == "radius":
                    cur.execute("SELECT market_name, city, state, y AS lat, x AS lon FROM farmers_markets")
                    results = []
                    for row in cur.fetchall():
                        if None in (row['lat'], row['lon']):
                            continue
                        dist = haversine(lat, lon, row['lat'], row['lon'])
                        if dist <= radius:
                            results.append(dict(row) | {"distance": round(dist, 1)})
                    markets = results
                else:
                    cur.execute(f"""
                        SELECT market_name, city, state, y AS lat, x AS lon
                        FROM farmers_markets
                        WHERE LOWER({search_type}) = %s
                    """, (query_value.lower(),))
                    markets = [dict(row) for row in cur.fetchall()]

            if not markets:
                print("\n❌ Рынки не найдены.")
            else:
                # Сортировка
                if sort_mode == 1:
                    markets.sort(key=lambda x: x['market_name'])
                elif sort_mode == 2:
                    markets.sort(key=lambda x: x['market_name'], reverse=True)
                elif sort_mode == 3:
                    # Добавим средний рейтинг
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT fm.market_name, COALESCE(AVG(r.rating), 0) AS avg_rating
                            FROM farmers_markets fm
                            LEFT JOIN reviews r ON fm.market_id = r.market_id
                            GROUP BY fm.market_name
                        """)
                        rating_map = {row['market_name']: row['avg_rating'] for row in cur.fetchall()}
                    for m in markets:
                        m['avg_rating'] = rating_map.get(m['market_name'], 0)
                    markets.sort(key=lambda x: x['avg_rating'], reverse=True)

                # Вывод
                print(f"\nНайдено: {len(markets)} рынков")
                for m in markets:
                    dist = f" | {m['distance']} миль" if 'distance' in m else ""
                    avg = m.get('avg_rating', 0)
                    stars = "★" * int(round(avg)) + "☆" * (5 - int(round(avg)))
                    print(f"📍 {m['market_name']} | {m['city']}, {m['state']} | {stars}{dist}")

                # Детали
                if input("\nОткрыть детали? (да/нет): ").strip().lower() in ['да', 'yes', 'y']:
                    name = input("Введите название рынка: ").strip()
                    if name:
                        farm_detail_information(name)

        except Exception as e:
            print(f"❌ Ошибка поиска: {e}")
        finally:
            conn.close()

# === Главное меню ===
def main_function():
    menu_dict = {
        1: [farm_list_function, "Просмотреть список всех фермерских рынков"],
        2: [search_farm_function, "Поиск по городу, штату, индексу или радиусу"],
        3: [farm_detail_information, "Подробная информация о рынке"],
        4: [input_feedback_function, "Оставить отзыв о рынке"],
        5: [delete_farm_function, "Удалить рынок"],
        0: [None, "Выйти из программы"]
    }

    print("🌾 Добро пожаловать в систему управления фермерскими рынками!\n")

    while True:
        choice = show_menu(menu_dict)
        flag, action = find_in_menu(menu_dict, choice)
        if flag:
            print(action)
            break
        else:
            if isinstance(action, str):
                print(action)
            else:
                action()


if __name__ == "__main__":
    main_function()