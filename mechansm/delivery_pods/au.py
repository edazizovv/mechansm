"""
Standard Bahnhoff-I-class delivery pod.
Modality Type: AU

NOTE: every Banhnhoff-I-class pod must implement all and only the following functions:
    1) consume
        Import sources in a modality type specific form and add them to current storage.
    2) pump
        Import standard storage-outloaded pack of items specific to the modality type.
    3) dump
        Export current storage into a standard pack.
"""


import os
import argparse
import psycopg2


def _process_video(links_to_read):

    names, authors, descriptions, ratings, thumbs, titles, nn_views, lengths, statuses = [], [], [], [], [], [], [], [], []

    for l in links_to_read:
        name = l[l.index('?v=') + len('?v='):]
        try:
            # commented out components can be added later should the need arise

            yy = YouTube(l)
            yy.check_availability()
            author = yy.author
            # description = get_description(yy)
            # rating = yy.rating
            thumb = yy.thumbnail_url
            title = yy.title
            # n_views = yy.views
            # length = yy.length
            status = "OK"

        except Exception as e:

            author = ''
            # description = ''
            # rating = numpy.nan
            thumb = ''
            title = ''
            # n_views = numpy.nan
            # length = numpy.nan
            status = "NOK"

        names.append(name)
        authors.append(author)
        descriptions.append(description)
        ratings.append(rating)
        thumbs.append(thumb)
        titles.append(title)
        nn_views.append(n_views)
        lengths.append(length)
        statuses.append(status)

    thumbnails = ['{0}.png'.format(name) for name in names]
    loaded_tab = pandas.DataFrame(data={
                                     '__name': names,
                                     '_name': titles,
                                     '_author': authors,
                                     # '_description': descriptions,
                                     '_link': links_to_read,
                                     'thumbnail': thumbnails,
                                     'status': statuses,
                                     'realm': "au",
                                     })

    return loaded_tab


def _process_thumbs(filtered_tab):

    result = filtered_tab.apply(
        func=lambda x: _get_thumb(name=x["name"], thumb=x["thumbnail"]),
        axis=1,
    )

    return


def _get_thumb(name, thumb):
    img_data = requests.get(thumb).content
    if not os.path.isdir('./thumb/'):
        os.mkdir("./thumb/")
    with open('./thumb/{0}.png'.format(name), 'wb') as handler:
        handler.write(img_data)
    return "OK"


def consume(file_path, db_connection):

    with open(file_path, "r", encoding="utf-8") as f:
        links_to_read = [line.strip() for line in f if line.strip()]

    loaded_tab = _process_video(links_to_read)

    read_sql_query = """
        SELECT *
        FROM public.pizzas
        WHERE realm = 'au'
        ;
    """
    existing_tab = pandas.read_sql(
        sql=read_sql_query,
        con=db_connection,
    )

    filtered_tab = (
        loaded_tab
            [
            loaded_tab
            ["_link"].isin(existing_tab["_link"].tolist())
        ]
        .copy()
    )

    _process_thumbs(
        filtered_tab=filtered_tab,
    )

    filtered_tab.to_sql(
        schema="public",
        name="pizzas",
        con=db_connection,
        if_exists="append",
    )

    return


def analyze(file_path, db_connection):
    print("Analyze function called")
    # TODO: implement analysis logic

def clean(file_path, db_connection):
    print("Clean function called")
    # TODO: implement cleanup logic

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_consume = subparsers.add_parser("consume", help="consume function")
    parser_consume.add_argument("file_path", help="Path to the input text file")
    parser_consume.add_argument("db_connection", help="Database connection string")

    # analyze command
    parser_analyze = subparsers.add_parser("analyze", help="Analyze links from a file")
    parser_analyze.add_argument("file_path", help="Path to the input text file")
    parser_analyze.add_argument("db_connection", help="Database connection string")

    # clean command
    parser_clean = subparsers.add_parser("clean", help="Clean links from a file")
    parser_clean.add_argument("file_path", help="Path to the input text file")
    parser_clean.add_argument("db_connection", help="Database connection string")

    args = parser.parse_args()

    conn = establish_connection(args.db_connection)

    if args.command == "consume":
        consume(args.file_path, conn)
    elif args.command == "analyze":
        analyze(args.file_path, conn)
    elif args.command == "clean":
        clean(args.file_path, conn)

    conn.close()


def establish_connection(connection_string):

    conn = psycopg2.connect(connection_string)

    return conn


if __name__ == "__main__":
    main()
