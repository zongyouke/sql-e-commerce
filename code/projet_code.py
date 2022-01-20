import pathlib
from typing import ItemsView
try:
    pathlib.Path("MyDataBase.db").unlink()
except FileNotFoundError:
    pass


import sqlite3
conn = sqlite3.connect("MyDataBase.db")
cur = conn.cursor()


######################################
#              FUNCTIONS             # 
######################################

def create_clean_db():
    '''
    Creates empty tables used in this database project.
    '''

    # USER table
    user = '''CREATE TABLE IF NOT EXISTS
              user (user_name TEXT);'''

    cur.execute(user)

    # ITEM table
    item = '''CREATE TABLE IF NOT EXISTS 
              item (categ_name TEXT,
                    item_name TEXT,
                    FOREIGN KEY(categ_name) REFERENCES category(categ_name),
                    PRIMARY KEY(item_name));'''

    cur.execute(item)

    # CATEGORY table
    categ = '''CREATE TABLE IF NOT EXISTS
               category (categ_name TEXT);'''

    cur.execute(categ)

    # SALE table
    sale = '''CREATE TABLE IF NOT EXISTS 
              sale (seller TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    price FLOAT,
                    FOREIGN KEY(seller) REFERENCES user(user_name),
                    FOREIGN KEY(item_name) REFERENCES item(item_name));'''

    cur.execute(sale)

    # ORDERLIST table
    order = '''CREATE TABLE IF NOT EXISTS 
               orderlist (order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                          buyer TEXT NOT NULL,
                          datetime TEXT NOT NULL,
                          FOREIGN KEY(buyer) REFERENCES user(user_name));'''

    cur.execute(order)

    # ORDERITEM table
    order_item = '''CREATE TABLE IF NOT EXISTS 
                    orderitem (datetime TEXT NOT NULL,
                               seller TEXT,
                               item_name TEXT,
                               quantity INTEGER,
                               FOREIGN KEY(datetime) REFERENCES orderlist(datetime));'''

    cur.execute(order_item)

    # BASKET table
    basket = '''CREATE TABLE IF NOT EXISTS 
                basket (buyer TEXT,
                        seller TEXT,
                        item_name TEXT,
                        quantity INTEGER,
                        FOREIGN KEY(seller) REFERENCES sale(seller),
                        FOREIGN KEY(item_name) REFERENCES sale(item_name));'''

    cur.execute(basket)

    # REVIEW table
    review = '''CREATE TABLE IF NOT EXISTS 
                review (user_name TEXT,
                        item_name TEXT,
                        seller TEXT,
                        rating INTEGER,
                        comments TEXT,
                        FOREIGN KEY(user_name) REFERENCES user(user_name),
                        FOREIGN KEY(seller) REFERENCES sale(seller),
                        FOREIGN KEY(user_name) REFERENCES user(user_name));'''

    cur.execute(review)

def drop_tables():
    '''
    Removes all created tables.
    ''' 

    r = '''DROP TABLE IF EXISTS user'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS item'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS category'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS sale'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS orderlist'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS orderitem'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS basket'''
    cur.execute(r)
    r = '''DROP TABLE IF EXISTS review'''
    cur.execute(r)

def insert_data(user_data, inventory_data):
    '''
    Inserts all given data into empty tables. 
    Args: 
        user_data (list): user names with purchase & sale records 
        inventory_data (list): item list
    '''

    # USER table 
    names = []
    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        buyer, seller, datetime = data[0], data[2], data[5]
        if datetime != "":
            if buyer != "" : names.append(buyer)
            if seller != "": names.append(seller)
            
    names = list(set(names))

    for name in names:
        cur.execute(f'''INSERT INTO user (user_name) 
                        VALUES ('{name}');''')

    conn.commit()


    # CATEGORY table
    tmp = inventory_data[0].replace('"', '')
    categ_items = tmp.strip('\n').split('\t')

    for item in categ_items:
        cur.execute(f'''INSERT INTO category (categ_name) 
                        VALUES ('{item}');''')

    conn.commit()


    # ITEM table
    all_items = []
    
    categs = inventory_data[0].replace('"','').strip('\n').split('\t')

    for line in inventory_data[1:]:
        all_items.append(line.replace('"','').strip('\n').split('\t'))

    for items in all_items:
        for categ, item in zip(categs, items):
            if item != "": 
                item = item + ' (' + categ + ')'
                cur.execute(f'''INSERT INTO item (categ_name, item_name) 
                                VALUES ("{categ}", "{item}");''')
    
    conn.commit()


    # SALE table
    sale_data = []
    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        sale_data.append((data[1], data[2], data[3]))
        sale_data = list(set(sale_data))
        
    for (item_name, seller, price) in sale_data:
        cur.execute(f'''INSERT INTO sale (seller, item_name, price) 
                        VALUES ("{seller}", "{item_name}", "{price}");''')

    conn.commit()


    # ORDERLIST table
    insert = []

    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        if data[5]!= "": insert.append((data[0], data[5].strip('"')))
        insert = list(set(insert))
        
    for (buyer,datetime) in insert:
        cur.execute(f'''INSERT INTO orderlist (buyer, datetime) 
                        VALUES ("{buyer}", "{datetime}");''')

    conn.commit()


    # ORDERITEM table
    insert = []

    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        if data[5]!= "": insert.append((data[1], data[2], data[4], data[5].strip('"')))
        insert = list(set(insert))
        
    for (item_name,seller,quantity,datetime) in insert:
        cur.execute(f'''INSERT INTO orderitem (datetime, seller, item_name, quantity) 
                        VALUES ("{datetime}", "{seller}", "{item_name}", {quantity});''')

    conn.commit()


    # BASKET table
    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        if data[5] == "":
            buyer, item_name, seller, quantity= data[0], data[1], data[2], data[4]
            cur.execute(f'''INSERT INTO basket (buyer, seller, item_name, quantity) 
                            VALUES ("{buyer}", "{seller}", "{item_name}", {quantity});''')

    conn.commit()

    # REVIEW table
    for line in user_data[1:]:
        data = line.strip('\n').split('\t')
        user_name, item_name, seller, rating, comments = data[0], data[1], data[2], data[6], data[7]
        if rating != "" and comments != "":
            cur.execute(f'''INSERT INTO review (user_name, item_name, seller, rating, comments) 
                            VALUES ("{user_name}", "{item_name}", "{seller}", {rating}, "{comments}");''')

    conn.commit()


######################################
#                MAIN                # 
######################################

# Read data first
with open("gladiator.tsv", "r") as f1:
    user_data = f1.readlines()

with open("inventaire.tsv", "r") as f2:
    inventory_data = f2.readlines()

# QA section
"""
Section 4.2
"""

create_clean_db()

"""
1. ajoutez les utilisateurs suivants : Alan, Béatrice, Corentin et Danielle. Affichez le
contenu de la table correspondante.
"""

names = ['Alan', 'Béatrice', 'Corentin', 'Danielle']

for name in names:
    cur.execute(f'''INSERT INTO user (user_name) 
                    VALUES ("{name}");''')
conn.commit()

cur.execute('''SELECT * FROM user;''')
print(cur.fetchall())

"""
2. ajoutez les articles suivants : granny smith (pomme), golden (pomme), poire belle
hélène (dessert), blanc manger coco (gâteau) et blanc manger coco (jeu). Affichez le
contenu de la table correspondante.
"""

items = ['granny smith (pomme)', 'golden (pomme)', 'poire belle hélène (dessert)', 'blanc manger coco (gâteau)', 'blanc manger coco (jeu)']

for item in items:
    start = item.find('(') + 1
    end = item.find(')')
    cur.execute(f'''INSERT INTO item (categ_name, item_name) 
                    VALUES ("{item[start:end]}","{item}");''')
conn.commit()

cur.execute('''SELECT * FROM item;''')
print(cur.fetchall())


"""
3. ajoutez des mises en ventes pour chacun de ces articles (au moins une par article) sur
différents utilisateurs. Ajoutez une mise en vente d’une granny smith par Danielle au
prix de votre choix.
"""

sales = [('Alan',('granny smith (pomme)', 'golden (pomme)')),
         ('Béatrice',('golden (pomme)', 'poire belle hélène (dessert)')),
         ('Corentin',('blanc manger coco (gâteau)', 'blanc manger coco (jeu)'))]

for (seller, items) in sales:
    for item in items:
        cur.execute(f'''INSERT INTO sale (seller, item_name) 
                        VALUES ("{seller}", "{item}");''')

# add Danielle's product and price
cur.execute('''INSERT INTO sale (seller, item_name, price) 
               VALUES ("Danielle", "granny smith (pomme)", 0.6);''')

conn.commit()

cur.execute('''SELECT * FROM sale;''')
print(cur.fetchall())


"""
4. ajoutez un avis sur la mise en vente d’une granny smith par Danielle (sans utiliser
directement son identifiant). La note doit être comprise entre 1 et 5.
"""

r = '''SELECT seller FROM sale
       WHERE seller = 'Danielle' AND item_name = 'granny smith';'''

for name_select in cur.execute(r):
    name = name_select[0]

cur.execute(f'''INSERT INTO review (seller, item_name, rating, comments) 
                VALUES ("{name}", "granny smith", 5, "parfait");''')

conn.commit()

cur.execute('''SELECT * FROM review;''')
print(cur.fetchall())

"""
5. affichez toutes les pommes mises en ventes triées par ordre lexicographique de nom
d’article puis d’utilisateur.
"""

r = '''SELECT item_name, seller FROM sale
       WHERE item_name IN (SELECT item_name FROM item 
                           WHERE categ_name = 'pomme')
       ORDER BY item_name;
    '''

cur.execute(r)
print(cur.fetchall())

"""
6. affichez le prix total de chaque article d’un panier de votre choix.
"""
# add items
cur.execute('''INSERT INTO sale (seller, item_name, price) 
               VALUES ("Corentin", "granny smith (pomme)", 0.3);''')
cur.execute('''INSERT INTO sale (seller, item_name, price) 
               VALUES ("Danielle", "golden (pomme)", 0.89);''')
conn.commit()

# put items into basket
cur.execute('''INSERT INTO basket (seller, item_name, quantity) 
               VALUES ("Danielle", "golden (pomme)", 10);''')
cur.execute('''INSERT INTO basket (seller, item_name, quantity) 
               VALUES ("Corentin", "granny smith (pomme)", 5);''')

conn.commit()

# calculate total price
r = f'''SELECT b.item_name, "total " || (s.price*b.quantity) || " EUR"
        FROM basket b
        LEFT JOIN sale s ON b.seller = s.seller AND b.item_name = s.item_name;'''

cur.execute(r)
print(cur.fetchall())


"""
7. affichez le prix total d’un panier de votre choix.
"""

r = f'''SELECT "total " || SUM(s.price*b.quantity) || " EUR"
        FROM basket b
        LEFT JOIN sale s ON b.seller = s.seller AND b.item_name = s.item_name;'''

cur.execute(r)
print(cur.fetchall())


"""
Section 5.2
"""

drop_tables()
create_clean_db()
insert_data(user_data,inventory_data)

"""""""""
1. combien y a-t-il d’utilisateurs ?
-> 26
"""
r = '''SELECT COUNT(user_name) FROM user;'''
print("1. combien y a-t-il d’utilisateurs ?")
print(cur.execute(r).fetchall())

"""
2. quel est l’article le plus cher et quel est son prix ?
-> raifort (légume) à 2 euros
"""
r = '''SELECT item_name, price FROM sale ORDER BY price DESC;'''
print("2. quel est l’article le plus cher et quel est son prix ?")
print(cur.execute(r).fetchall()[0])

"""
3. combien d’articles recensés ne sont vendus par personne ?
-> 677
"""

r = '''SELECT item_name FROM item 
       EXCEPT
       SELECT item_name FROM sale;'''
print("3. combien d’articles recensés ne sont vendus par personne ?")
print(len(cur.execute(r).fetchall()))

"""
4. qui a le plus grand nombre d’articles (total des quantités) dans son panier ?
-> Corentin, avec 30 articles
"""
r = '''SELECT buyer, SUM(quantity) FROM basket
       GROUP BY buyer;'''
print("4. qui a le plus grand nombre d’articles (total des quantités) dans son panier ?")
print(cur.execute(r).fetchall()[0])

"""
5. quels sont les articles présents dans le panier de Xian et en quelle quantité ?
-> [('maigold (pomme)', 11), ('konbu (légume)', 5), ('frisée (légume)', 3), ('granny smith (pomme)', 3), ('cara (pomme de terre)', 1)]
"""

r = '''SELECT item_name, quantity FROM basket
       WHERE buyer = "Xian"
       ORDER BY quantity DESC;'''
print("5. quels sont les articles présents dans le panier de Xian et en quelle quantité ?")
print(cur.execute(r).fetchall())


"""
6. de quel vendeur-euse Erwan a-t-il le plus d’articles dans son panier ?
-> Hélène, avec 18 articles
"""
r = '''SELECT seller, SUM(quantity) as sum FROM basket
       WHERE buyer = "Erwan"
       GROUP by seller
       ORDER BY sum DESC;'''
print("6. de quel vendeur-euse Erwan a-t-il le plus d’articles dans son panier ?")
print(cur.execute(r).fetchall()[0])


"""
7. de quand date la dernière transaction de Vera ?
-> 1993-10-23 17:25:40
"""
r = '''SELECT datetime FROM orderlist
       WHERE buyer = "Vera"
       ORDER BY datetime DESC;'''
print("7. de quand date la dernière transaction de Vera ?")
print(cur.execute(r).fetchall()[0])


"""
8. quel(s) article(s) a/ont la meilleure note moyenne ?
-> vitelotte noire (pomme de terre) à 5.0
"""
r = '''SELECT item_name, AVG(rating) FROM review
       GROUP BY item_name
       ORDER BY rating DESC;'''
print("8. quel(s) article(s) a/ont la meilleure note moyenne ?")
print(cur.execute(r).fetchall()[0])


"""
9. qui a fait le plus d’achats et combien y en a-t-il ?
-> Jeanne avec 29 achats
"""
r = '''SELECT buyer, COUNT(buyer) as count FROM orderlist
       GROUP BY buyer
       ORDER BY count DESC;'''
print("9. qui a fait le plus d’achats et combien y en a-t-il ?")
print(cur.execute(r).fetchall()[0])


"""
10. qui est le vendeur le plus populaire (dont on a acheté le plus d’articles) ?
-> Corentin, avec 717 articles vendus
"""
r = '''SELECT seller, SUM(quantity) as count FROM orderitem
       GROUP BY seller
       ORDER BY count DESC;'''
print("10. qui est le vendeur le plus populaire (dont on a acheté le plus d’articles)?")
print(cur.execute(r).fetchall()[0])

conn.close()
