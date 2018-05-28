import os #For looking through files
import os.path #For managing file paths
import bs4 #CLMET3 Documents are stored in xml format
import pandas
import nltk

dataDir = '/Users/lingdai/Downloads/clmet/corpus/txt/plain/' #change data directory

#Migrate data into pandas DataFrame
CLMET3DF = pandas.DataFrame()

DocDict = {"id":[],
           "file":[],
           "period":[],
           "quartcent":[],
           "decade":[],
           "year":[],
           "genre":[],
           "subgenre":[],
           "title":[],
           "author":[],
           "gender":[],
           "text":[]}

for Doc in ((dataDir + file) for file in os.listdir(dataDir) if file.endswith(".txt")):
    file = open(Doc, "r")
    soup = bs4.BeautifulSoup(file.read(), "html.parser")
    DocDict['id'].append(soup.find("id").text)
    DocDict['file'].append(soup.find("file").text)
    DocDict['period'].append(soup.find("period").text) #file_name
    DocDict['quartcent'].append(soup.find("quartcent").text) #file_name
    DocDict['decade'].append(soup.find("decade").text) #file_name
    DocDict['year'].append(soup.find("year").text) #file_name
    DocDict['genre'].append(soup.find("genre").text) #file_name
    DocDict['subgenre'].append(soup.find("subgenre").text) #file_name
    DocDict['title'].append(soup.find("title").text) #file_name
    DocDict['author'].append(soup.find("author").text) #file_name
    DocDict['gender'].append(soup.find("gender").text) #file_name
    DocDict['text'].append(soup.find("text").text) #file_name

CLMET3DF = pandas.DataFrame(DocDict)

#Reorganize DataFrame
CLMET3DF = CLMET3DF.convert_objects(convert_numeric=True)
CLMET3DF = CLMET3DF.sort_values(by = ['id'])
CLMET3DF = CLMET3DF[['id', 'file', 'title', 'year', 'period','quartcent','decade',
                     'author','gender','genre','subgenre','text']]

#Tokenization of words
CLMET3DF['tokenized_sents'] = CLMET3DF['text'].apply(lambda x: [nltk.word_tokenize(s) for s in nltk.sent_tokenize(x)])
CLMET3DF['normalized_sents'] = CLMET3DF['tokenized_sents'].apply(lambda x: [lucem_illud.normalizeTokens(s, stopwordLst = lucem_illud.stop_words_basic) for s in x])
