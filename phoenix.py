import phoenixdb
import phoenixdb.cursor
import pandas as pd
from sqlalchemy import create_engine

# TODO: 1. Remove [index] column in the "shortened link" dataframe. 2. Write the "shortened link" dataframe to a table in MySQL.  


def return_connection_string(database_name, database_user, database_password, database_host, database_port):
    try:
        connection_string = 'mysql+mysqlconnector://' + database_user + ':' + \
            database_password + '@' + database_host + ':' + database_port + '/' + database_name
        print(connection_string)
        return connection_string
    except Exception as e:
        print('Encountered error while generating connection string for MySQL!')
        print(e)


def return_connection_object(database_name, database_user, database_password, database_host, database_port):
    try:
        connection_string = return_connection_string(
            database_name, database_user, database_password, database_host, database_port)
        engine = create_engine(connection_string).connect()
        return engine
    except Exception as e:
        print('Encountered error while connecting to MySQL database!')
        print(e)


"""Harcoded URL to BIDW MySQL. Must be parameterized."""
mysql_conn = return_connection_object(
    '?', '?', '?', '?', '?')

"""Harcoded URL to Phoenix HBase. Must be parameterized."""
database_url = 'http://?-?.?.?.?:?'


try:
    """Connection to HBase."""
    conn = phoenixdb.connect(database_url, autocommit=True)

    """Query to get short URLs."""
    sql_get_hits = "SELECT \"backwards_compatible_short_urls_shortUrl\", \"child_id\", \"date\",\"headers_accept\", \"headers_xforwardedproto\", \"userAgent\", \"headers_acceptencoding\",\"ipAddr\", \"headers_via\", \"referrer\", \"headers_useragent\", \"headers_acceptlanguage\", \"headers_host\" FROM \"?\".\"backwards_compatible_short_urls_hits\" WHERE \"date\" >=now()-1"

    """Execute the query by pulling hits data for yesterday."""
    get_hits = pd.read_sql_query(sql_get_hits, conn)

    """The query results are stored in a dataframe."""
    get_hits_df = pd.DataFrame(get_hits, columns=['backwards_compatible_short_urls_shortUrl', 'child_id', 'date', 'headers_accept', 'headers_xforwardedproto',
                                                  'userAgent', 'headers_acceptencoding', 'ipAddr', 'headers_via', 'referrer', 'headers_useragent', 'headers_acceptlanguage', 'headers_host'])

    """This section will iterate thru the entire dataframe."""
    for index, row in get_hits_df.iterrows():

        """The query pulls the shortened links based on the hash ie shorturl value."""
        sql_shortenedlink = "SELECT \"OutgoingMessageID\", \"ID\", \"Hash\", \"clicked\", \"LongUrl\", \"CreatedAt\", \"PhoneNumber\", \"Deleted\", \"UpdatedAt\" FROM \"?\".\"ShortenedLinksForOutgoingMessages\" WHERE \"Hash\" = '" + \
            row['backwards_compatible_short_urls_shortUrl'] + "'"

        """Executes the query."""
        get_shortlinks = pd.read_sql_query(sql_shortenedlink, conn)

        get_shortlinks_df = pd.DataFrame(get_shortlinks, columns=[
            'OutgoingMessageID', 'ID', 'Hash', 'clicked', 'LongUrl', 'CreatedAt', 'PhoneNumber', 'Deleted', 'UpdatedAt'])

        """Presenting the whole dataframe."""
        if not get_shortlinks_df.empty:
            get_shortlinks_df.to_sql('ShortenedLinksForOutgoingMessages',
                                     mysql_conn, if_exists='append')

except Exception as e:
    print(e)
finally:
    print("Process Complete.")
    mysql_conn.close()
