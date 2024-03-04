#-------------------------------------------------------------------------
# AUTHOR: Alondra Marin
# FILENAME: db_connection.py
# SPECIFICATION: This script interacts with a PostgreSQL database using psycopg2, 
# allowing for the management of document categories, content insertion, 
# deletion, updating, and term index retrieval.
# FOR: CS 4250- Assignment #1
# TIME SPENT: This took me a few days to complete.
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays



#importing some Python libraries
import psycopg2

def connectDataBase():
    # Create a database connection object using psycopg2
    try:
        # Terminal command:  ps aux | grep postgres
        conn = psycopg2.connect(database="corpus", user="postgres", password="921401", host="localhost", port="5432")
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None

def createCategory(cur, catId, catName):
    # Insert a category in the database
    try:
        cur.execute("INSERT INTO categories (category_id, name) VALUES (%s, %s)", (catId, catName))
    except psycopg2.Error as e:
        print("Error creating the category:", e)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    try:
        # 1 Get the category id based on the informed category name
        cur.execute("SELECT category_id FROM categories WHERE name = %s", (docCat,))
        category_id = cur.fetchone()[0]

        # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
        cleaned_string = ''
        for char in docText:
            if char.isalnum():
                cleaned_string += char
        num_chars = len(cleaned_string)

        cur.execute("INSERT INTO documents (doc_id, text, title, num_chars, date, category_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (docId, docText, docTitle, num_chars, docDate, category_id))

        # 3 Update the potential new terms.
        # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
        # 3.2 For each term identified, check if the term already exists in the database
        # 3.3 In case the term does not exist, insert it into the database
        terms = [term.strip('.,!?').lower() for term in docText.split()] # lowercase and remove punctuation
        for term in terms:
            num_chars = len(term)
            cur.execute("INSERT INTO terms (term, num_chars) VALUES (%s, %s) ON CONFLICT DO NOTHING", (term, num_chars,))

        # 4 Update the index
        # 4.1 Find all terms that belong to the document
        # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        # 4.3 Insert the term and its corresponding count into the database
        term_counts = {}
        for term in terms:
            if term in term_counts:
                term_counts[term] += 1
            else:
                term_counts[term] = 1

        for term, count in term_counts.items():
            cur.execute("INSERT INTO index (term, doc_id, count) VALUES (%s, %s, %s)",
                        (term, docId, count))
    except psycopg2.Error as e:
        print("Error creating the document:", e)

def deleteDocument(cur, docId):
    try:
        # 1 Query the index based on the document to identify terms
        # 1.1 For each term identified, delete its occurrences in the index for that document
        # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
        cur.execute("SELECT term FROM index WHERE doc_id = %s", (docId,))
        terms = cur.fetchall()

        # 2 Delete the document from the database
        cur.execute("DELETE FROM documents WHERE doc_id = %s", (docId,))

        # Update the index by decrementing the counts of terms
        for term in terms:
            cur.execute("UPDATE index SET count = count - 1 WHERE term = %s", (term[0],))
            
            # Check if there are no more occurrences of the term in other documents and delete it from the index
            cur.execute("DELETE FROM index WHERE term = %s AND count <= 0", (term[0],))

    except psycopg2.Error as e:
        print("Error deleting the document:", e)

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1 Delete the document
        cur.execute("SELECT term FROM index WHERE doc_id = %s", (docId,))
        terms = cur.fetchall()
        cur.execute("DELETE FROM documents WHERE doc_id = %s", (docId,))

        # 2 Create the document with the same id
        cur.execute("SELECT category_id FROM categories WHERE name = %s", (docCat,))
        category_id = cur.fetchone()[0]
        
        cleaned_string = ''
        for char in docText:
            if char.isalnum():
                cleaned_string += char
        num_chars = len(cleaned_string)

        cur.execute("INSERT INTO documents (doc_id, text, title, num_chars, date, category_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (docId, docText, docTitle, num_chars, docDate, category_id))
        
        # Update the potential new terms
        terms_to_insert = [term.strip('.,!?').lower() for term in docText.split()]  # lowercase and remove punctuation
        for term in terms:
            num_chars = len(term)
            cur.execute("INSERT INTO terms (term, num_chars) VALUES (%s, %s) ON CONFLICT DO NOTHING", (term, num_chars,))

        # Update the index for terms in the new document
        term_counts = {}
        for term in terms_to_insert:
            if term in term_counts:
                term_counts[term] += 1
            else:
                term_counts[term] = 1

        for term, count in term_counts.items():
            cur.execute("INSERT INTO index (term, document_id, count) VALUES (%s, %s, %s)",
                        (term, docId, count))
            
        # Update the index for terms that were associated with the document (decrement the count)
        for term in terms:
            cur.execute("UPDATE index SET count = count - 1 WHERE term = %s", (term[0],))
            cur.execute("DELETE FROM index WHERE term = %s AND count <= 0", (term[0],))

    except psycopg2.Error as e:
        print("Error updating the document:", e)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    inverted_index = {}
    try:
        # Modify the SQL query to match your table structure
        cur.execute("SELECT term, array_agg(documents.title || ':' || count) FROM index INNER JOIN documents ON index.doc_id = documents.doc_id GROUP BY term ORDER BY term")
        rows = cur.fetchall()
        for row in rows:
            term, counts = row
            inverted_index[term] = ','.join(counts)
    except psycopg2.Error as e:
        print("Error retrieving the inverted index:", e)
    return inverted_index