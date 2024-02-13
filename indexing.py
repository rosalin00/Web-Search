#-------------------------------------------------------------------------
# AUTHOR: Alondra Marin
# FILENAME: indexing.py
# SPECIFICATION:
# FOR: CS 4250- Assignment #1
# TIME SPENT: It took me about two days
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays

#importing some Python libraries
import csv
import math

documents = []
labels = []

#reading the data in a csv file
with open('collection.csv', 'r') as csvfile:
  reader = csv.reader(csvfile)
  for i, row in enumerate(reader):
         if i > 0:  # skipping the header
            documents.append (row[0])
            labels.append(row[1])

#Conduct stopword removal.
#--> add your Python code here
stopWords = {'I', 'and', 'She', 'They', 'her', 'their'}

for document in documents:
  index = documents.index(document)
  documents[index] = document.split(" ")

for document in documents:
  for term in document:
    if term in stopWords:
      document.remove(term)
# Output: [['love', 'cats', 'cats'], ['loves', 'dog'], ['love', 'dogs', 'cats']]

#Conduct stemming.
#--> add your Python code here
steeming = {
  "cats": "cat",
  "dogs": "dog",
  "loves": "love",
}
for document in documents:
  for term in document:
    index = document.index(term)
    if term in steeming.keys():
      term = steeming[term]
    document[index] = term
# Output: [['love', 'cat', 'cat'], ['love', 'dog'], ['love', 'dog', 'cat']]

#Identify the index terms.
#--> add your Python code here
terms = []
for document in documents:
    for term in document:
        if term not in terms:
            terms.append(term)

#Build the tf-idf term weights matrix.
#--> add your Python code here
docMatrix = []
for document in documents:
    doc_row = []
    for term in terms:
        tf = document.count(term) / len(document)
        df = sum(1 for doc in documents if term in doc)
        idf = math.log10(len(documents) / df)
        tfidf = tf * idf
        doc_row.append(tfidf)
    docMatrix.append(doc_row)
# Output: [[0.0, 0.11739417270378749, 0.0], [0.0, 0.0, 0.08804562952784062], [0.0, 0.058697086351893746, 0.058697086351893746]]


#Calculate the document scores (ranking) using document weigths (tf-idf) calculated before and query weights (binary - have or not the term).
#--> add your Python code here
query = "cat and dogs"
query_terms = ['cat', 'dog']

docScores = []
for i, document in enumerate(documents):
    score = 0
    for term in query_terms:
        if term in terms:
            term_index = terms.index(term)
            score += docMatrix[i][term_index]
    docScores.append(score)
# docScores = [0.11739417270378749, 0.08804562952784062, 0.11739417270378749]

#Calculate and print the precision and recall of the model by considering that the search engine will return all documents with scores >= 0.1.
#--> add your Python code here
threshold = 0.1

relevant_hits = 0
relevant_misses = 0
irrelevant_noise = 0

for score in docScores:
   if score > threshold:
      score_index = docScores.index(score)
      label = labels[score_index]
      if label == 'R':
         relevant_hits += 1
      elif label == 'I':
         irrelevant_noise += 1
   else:
      score_index = docScores.index(score)
      label = labels[score_index]
      if label == 'R':
         relevant_misses += 1

recall = ((relevant_hits)/(relevant_hits+relevant_misses)) * 100
precision = ((relevant_hits)/(relevant_hits+irrelevant_noise)) * 100

print("Precision: {}%".format(precision))
print("Recall: {}%".format(recall))
# Output: Precision: 100.0% Recall: 100.0%
