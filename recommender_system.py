import mysql.connector


def mysql_connector(user, password, db_name):
    """
    Create a connection with my Sql database.
    :param user:
    :param password:
    :param db_name:
    :return:
    """
    # Create connection with database
    db = mysql.connector.connect(host="localhost", user=user, password=password, database=db_name)
    # Create cursor
    cursor = db.cursor()
    return db, cursor


def sql_closer(db, cursor):
    """
    Commit and close connection with my Sql database.
    :param db:
    :param cursor:
    """
    # Close the cursor and commit changes made in the database
    cursor.close()
    db.commit()
    db.close()


def create_rule_table(cursor, table_name):
    """
    Create table with name variable "table_name" for to hold the recommendations.
    :param cursor:
    :param table_name:
    """
    # Create a table if it not already exists
    query = "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(255) PRIMARY KEY UNIQUE, product_one VARCHAR(255)" \
           ", product_two VARCHAR(255), product_three VARCHAR(255), product_four VARCHAR(255))""" % table_name
    cursor.execute(query)


def delete_table(cursor, table_name):
    """
    Delete table with variable "table_name".
    :param cursor:
    :param table_name:
    """
    # Drop the table if it exists.
    query = "DROP TABLE IF EXISTS %s" % table_name
    cursor.execute(query)


def insert_into_table(cursor, insert_list, table):
    """
    Insert list of products into mysql database.
    :param cursor:
    :param insert_list:
    :param table:
    :return:
    """
    # create a query string with table varible
    query = "INSERT INTO "+table+" (id, product_one,  product_two, product_three, product_four) VALUES (%s, %s, %s, %s, %s) "
    # for each item in insert list
    for item in insert_list:
        value = (item[0], item[1][0], item[1][1], item[1][2], item[1][3])
        cursor.execute(query, value)
    return


def retrieve_data_query(direction, cursor, select, *condition):
    """
    Retrieve data from mysql with query. direction determines which query is selected. The conditions are the items to retrieve.
    from the database. execute the query and return the list of items
    :param direction: Determines what query to select
    :param cursor: My Sql cursor
    :param select: Items to select
    :param condition: all conditions
    :return:
    """
    # If direction is the same as x
    if direction == 0:
        # Create a string query with received variables
        query = "SELECT %s FROM %s" % (select, condition[0])
    elif direction == 1:
        query = "SELECT DISTINCT %s FROM %s, %s, %s WHERE %s = %s AND %s = %s" \
                 % (select, condition[0], condition[1], condition[2], condition[3], condition[4], condition[5], condition[6])
    elif direction == 2:
        query = "SELECT %s FROM %s, %s WHERE %s = %s AND %s = %s" \
                 % (select, condition[0], condition[1], condition[2], condition[3], condition[4], condition[5])
    elif direction == 3:
        query = "SELECT %s, %s, %s, %s, %s  FROM %s WHERE %s = %s" \
                 % (select, condition[0], condition[1], condition[2], condition[3], condition[4], condition[5], condition[6])
    elif direction == 4:
        query = "SELECT %s FROM %s WHERE %s = %s OR %s = %s OR %s = %s OR %s = %s OR %s = %s" \
                 % (select, condition[0], condition[1], condition[2], condition[3], condition[4], condition[5], condition[6],
                    condition[7], condition[8], condition[9], condition[10])
    elif direction == 5:
        query = "SELECT DISTINCT %s FROM %s, %s WHERE %s = %s" % (select, condition[0], condition[1], condition[2], condition[3])
    elif direction == 6:
        query = "SELECT %s FROM %s WHERE %s = %s AND %s = %s AND %s = %s AND %s = %s AND %s = %s" \
                 % (select, condition[0], condition[1], condition[2], condition[3], condition[4], condition[5], condition[6],
                    condition[7], condition[8], condition[9], condition[10])
    elif direction == 7:
        query = "SELECT %s FROM %s WHERE %s = %s" % (select, condition[0], condition[1], condition[2])

    # Execute the query
    cursor.execute(query)
    # Retrieve all data
    data = cursor.fetchall()
    return data


def retrieve_order_events_products(direction, cursor, profile_list):
    """
    Retrieve for each profile all products. Depending on the direction it will retrieve either order products id key's or
    web events id key's. Append each product in a list with profile number.
    :param direction:
    :param cursor:
    :param profile_list:
    :return:
    """
    profile_product_list = []
    # for eacht profile
    for item in profile_list:
        # Create a new_string for query ' are needed.
        new_item = "'" + str(item[0]) + "'"
        if direction == 0:
            # retrieve all order products id
            data = retrieve_data_query(2, cursor, "orders.products_id_key", "orders", "sessions",
                                       "orders.sessions_id_key",
                                       "sessions.id", "sessions.profiles_id_key", new_item)
        if direction == 1:
            # retrieve all web_event products
            data = retrieve_data_query(2, cursor, "web_events.products_id_key", "web_events", "sessions",
                                       "web_events.sessions_id_key",
                                       "sessions.id", "sessions.profiles_id_key", new_item)
        profile_product_list.append([item[0], retrieve_correct(data)])
    return profile_product_list


def retrieve_correct(data):
    """
    Return de all products from different sessions in one list
    :param data:
    :return:
    """
    # Create a list
    product_list = []
    # Loop trough a list of lists.
    for products in data:
        # loop trough all items
        for product in products:
            # append item in product_list
            product_list.append(product)
    return product_list


def retrieve_similar_category_products(cursor, products):
    """
    Retrieve all te category's beloning to the product.
    Return two list of items. One containing all products that have all 5 categories in common with the given product. And
    one containing all the products that have at least 1 category in common with the given product.
    :param cursor:
    :param products:
    :return:
    """
    # Create a new_string for query ' are needed.
    new_item = "'" + str(products) + "'"
    # Retrieve all categories belonging to the product
    product_category = retrieve_data_query(3, cursor, "products.gender_id_key", "products.doelgroep_id_key","products.brand_id_key",
                                           "products.main_category_id_key", "products.sub_category_id_key", "products", "products.id", new_item)
    # Retrieve all products that have one category in common
    relative_similar_products= retrieve_data_query(4, cursor, "products.id", "products", "products.gender_id_key",
                                                   product_category[0][0], "products.doelgroep_id_key", product_category[0][1],
                                                   "products.brand_id_key", product_category[0][2], "products.main_category_id_key", product_category[0][3],
                                                   "products.sub_category_id_key", product_category[0][4])
    # Retrieve all products that have all categories in common
    similar_products = retrieve_data_query(6, cursor, "products.id", "products", "products.gender_id_key",
                                                   product_category[0][0], "products.doelgroep_id_key",
                                                   product_category[0][1],
                                                   "products.brand_id_key", product_category[0][2],
                                                   "products.main_category_id_key", product_category[0][3],
                                                   "products.sub_category_id_key", product_category[0][4])
    return similar_products, relative_similar_products


def frequency_category(product, freq_dict, product_category, num):
    """
    To determine what recommendation should be given each product is counted and given a value based on the most
    in common product. Items that are already bought are removed.
    :param product:
    :param freq_dict:
    :param product_category:
    :return:
    """
    # for item in product_category
    for item in product_category:
        # if the item is already in products
        if item[0] not in freq_dict and item[0] not in product:
            # add item in dict and add given value
            freq_dict[item[0]] = num
        elif item[0] in freq_dict:
            freq_dict[item[0]] += num
    return freq_dict


def merge_frequency(item_dict, second_item_dict):
    """
    Merging 2 dictionary's with items and values.
    :param item_dict:
    :param second_item_dict:
    :return:
    """
    # for item in the first dictionary
    for item in item_dict:
        # if item in the second dictionary
        if item in second_item_dict:
            item_dict[item] += second_item_dict[item]
    return item_dict


def common_product_category_or_profile(direction, cursor, profile_products_list):
    """
    if direction is 0 retrieve for each profile all items that have either all or one category in common.
    if direction is 1 retrieve for each profile all products id with similar bought items.
    Determine the frequency and add a value depending on the how much in common item has with the original profile.
    sort the dictionary and return the 4 most repeating items depening on value.
    :param cursor:
    :param profile_products_list:
    :return:
    """
    profile_recomondation = []
    # for each profile
    for profile in profile_products_list:
        recomondation_product = {}
        # for each product in profile
        for product in profile[1]:
            if direction == 0:
                # Retrieve similar and relative similar category items. (either one category in common or all category's)
                similar_product, relative_products = retrieve_similar_category_products(cursor, product)
                # Reduce duplicates
                relative = frequency_category(product, recomondation_product, relative_products, 0.01)
                similar = frequency_category(product, recomondation_product, similar_product, 0.05)
                # Merge the two dictionary's
                recomondation_product = merge_frequency(relative, similar)
            if direction == 1:
                # Retrieve similar profile items.
                similar_profile_products = retrieve_similar_profile_products(cursor, product)
                # Reduce duplicates
                recomondation_product = frequency_category(product, recomondation_product, similar_profile_products, 0.01)
        # Sort the dictionary and return the first 4 items
        products = sorted(recomondation_product, key=recomondation_product.get, reverse=True)[:4]
        profile_recomondation.append([profile[0], products])

    return profile_recomondation


def retrieve_similar_profile_products(cursor, product):
    """
    Retrieve for each product all sessions that bought the same item. for each session return all items bought.
    :param cursor:
    :param product:
    :return:
    """
    # Create a new_string for query ' are needed.
    new_item = "'" + str(product) + "'"
    # Retrieve all sessions with the same item bought
    profiles = retrieve_data_query(7, cursor, "web_events.sessions_id_key", "web_events", "web_events.products_id_key", new_item)
    profile_products_list = []
    # for each profile
    for item in profiles:
        # Create a new_string for query ' are needed.
        new_item = "'" + str(item[0]) + "'"
        # for each sessions return all items bought
        profile_items = retrieve_data_query(7, cursor, "web_events.products_id_key", "web_events","web_events.sessions_id_key", new_item)
        # add the list to profile_products_list
        profile_products_list += list(profile_items)
    return profile_products_list


def retrieve_web_events_profiles(cursor):
    """
    Return all profiles with web events.
    :param cursor:
    :return:
    """
    profiles_events = retrieve_data_query(5, cursor, "sessions.profiles_id_key ", "web_events", "sessions", "sessions.id", "web_events.sessions_id_key")
    return profiles_events


def retrieve_order_profiles(cursor):
    """
    Return all profiles with orders.
    :param cursor:
    :return:
    """
    profiles_orders = retrieve_data_query(5, cursor, "sessions.profiles_id_key", "orders", "sessions", "sessions.id",
                                          "orders.sessions_id_key")
    return profiles_orders


# ------------------ recommendation process -------------------


def recommendation_order_category_process():
    """
    Rule: Similar to your bought items.     (Content filtering)
    This process creates 4 recommendations based on the already purchased orders. For each product in order return all
    products which are similar to the profile orders in reference to all and one of the following categories: main_category,
    sub_category, gender, doelgroep and brand. For all the returned products determine the frequency and add a value for
    each product to determine which product highest recommended and return the 4 most common product items.
    """
    # Create connection with to mysql database.
    db, cursor = mysql_connector("root", "", "dbhu")
    # Retrieve all profiles that have orders.
    profiles_orders = retrieve_order_profiles(cursor)
    # profiles_orders = retrieve_data_query(5, cursor, "sessions.profiles_id_key", "orders", "sessions", "sessions.id", "orders.sessions_id_key")

    # Retrieve all products id's for each profile.
    profile_products_list = retrieve_order_events_products(0, cursor, profiles_orders)
    # Retrieve a list of profiles with four items.
    profile_product_ids = common_product_category_or_profile(0, cursor, profile_products_list)

    # Delete table "content_filer_order_category" if it already exist.
    delete_table(cursor, "content_filer_order_category")
    # Create table "content_filer_order_category"
    create_rule_table(cursor, "content_filer_order_category")
    # Insert all data into table
    insert_into_table(cursor, profile_product_ids, "content_filer_order_category")
    # Commit and close database connection
    sql_closer(db, cursor)


def recommendation_events_category_process():
    """
    Rule: You may also like.        (Content filtering)
    This process creates 4 recommendations based on the web events. For each product in events return all
    products which are similar to the profile events in reference to all and one of the following categories: main_category,
    sub_category, gender, doelgroep and brand. For all the returned products determine the frequency and add a value for
    each product to determine highest recommended. Return the 4 most common product items.
    """
    # Create connection with to mysql database.
    db, cursor = mysql_connector("root", "", "dbhu")
    # Retrieve all profiles that have events.
    profiles_events = retrieve_web_events_profiles(cursor)
    # profiles_events = retrieve_data_query(5, cursor, "sessions.profiles_id_key ", "web_events", "sessions", "sessions.id", "web_events.sessions_id_key")

    # Retrieve all products id's for each profile.
    profile_products_list = retrieve_order_events_products(1, cursor, profiles_events)
    # Retrieve a list of profiles with four items.
    profile_product_ids = common_product_category_or_profile(0, cursor, profile_products_list)

    # Delete table "content_filer_event_category" if it already exist.
    delete_table(cursor, "content_filer_event_category")
    # Create table "content_filer_event_category"
    create_rule_table(cursor, "content_filer_event_category")
    # Insert all data into table
    insert_into_table(cursor, profile_product_ids, "content_filer_event_category")
    # Commit and close database connection
    sql_closer(db, cursor)


def recommendation_profile_events_process():
    """
    Rule: People similar to you looked at. (Collaborative Filtering)
    This process creates 4 recommendations based on the web events. For each profile with events return all
    products. For each product in profiles retrieve all sessions with similar products and determine the frequency
    and add a value for each product to determine highest recommended. Return the 4 most common product items.
    """
    # Create connection with to mysql database.
    db, cursor = mysql_connector("root", "", "dbhu")
    # Retrieve all profiles that have events.
    profiles_events = retrieve_web_events_profiles(cursor)

    # Retrieve all products id's for each profile.
    profile_products_list = retrieve_order_events_products(1, cursor, profiles_events)
    # # Retrieve a list of profiles with four items.
    products = common_product_category_or_profile(1, cursor, profile_products_list)

    # Delete table "collaborative_filer_event_category" if it already exist.
    delete_table(cursor, "collaborative_filer_event_category")
    # Create table "collaborative_filer_event_category"
    create_rule_table(cursor, "collaborative_filer_event_category")
    # Insert all data into table
    insert_into_table(cursor, products, "collaborative_filer_event_category")
    # Commit and close database connection
    sql_closer(db, cursor)


def recommendation_profile_orders_process():
    """
    Rule: People similar to you bought. (Collaborative Filtering)
    This process creates 4 recommendations based on the orders. For each profile with orders return all
    products. For each product in profiles retrieve all sessions with similar products and determine the frequency
    and add a value for each product to determine highest recommended. Return the 4 most common product items.
    """
    # Create connection with to mysql database.
    db, cursor = mysql_connector("root", "", "dbhu")
    # Retrieve all profiles that have orders.
    profiles_orders = retrieve_order_profiles(cursor)

    # Retrieve all products id's for each profile.
    profile_products_list = retrieve_order_events_products(0, cursor, profiles_orders)
    # Retrieve a list of profiles with four items.
    products = common_product_category_or_profile(1, cursor, profile_products_list)

    # Delete table "collaborative_filer_order_category" if it already exist.
    delete_table(cursor, "collaborative_filer_order_category")
    # Create table "collaborative_filer_order_category"
    create_rule_table(cursor, "collaborative_filer_order_category")
    # Insert all data into table
    insert_into_table(cursor, products, "collaborative_filer_order_category")
    # Commit and close database connection
    sql_closer(db, cursor)


# recommendation_profile_orders_process()
# recommendation_profile_events_process()
# recommendation_events_category_process()
# recommendation_order_category_process()
