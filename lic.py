import csv
import math as m
from datetime import datetime as dt
import psycopg2
import sys


# PART 1: DATA PREPARATION

# Connect to existing database
db_conn = psycopg2.connect("dbname=postgres user=postgres")

# Open a cursor to perform database operations
c = db_conn.cursor()

# Create table
c.execute('''CREATE TABLE chess_data
             (id serial PRIMARY KEY, date TEXT, nameW TEXT, nameB TEXT, whiteRank INT, blackRank INT, tournament TEXT, t_round INT, result REAL);''')


nameW = list()
nameB = list()
whiteRank = list()
blackRank = list()
result = list()
tournament_data = list()
date = list()
month = list()
day = list()
year = list()
t_round = list()
tournament = list()

print('Data loading initiated')
with open('/home/janek/Documents/licencjat/base.csv', newline='', encoding='ISO-8859-1') as csvfile:
    data = csv.reader(csvfile, delimiter=',')
    for row in data:
        try:
            date.append(dt.strptime(row[8], '%d.%m.%Y'))
        except:
            continue
        nameW.append(row[0] + ' ' + row[1])
        nameB.append(row[3] + ' ' + row[4])
        whiteRank.append(int(row[2]))
        blackRank.append(int(row[5]))
        result.append(float(row[9]))
        tournament_data = row[7].split(sep=' ')
        try:
            t_round.append(int(m.floor(float(tournament_data[-1][1:-1]))))
            tournament.append(' '.join(tournament_data[:-1]))
        except:
            t_round.append(None)
            tournament.append(' '.join(tournament_data))
print('All data loaded, commence feature engineering')

# Insert a row of data
for i, elem in enumerate(nameW):
    try:
        row = (date[i], nameW[i], nameB[i], whiteRank[i], blackRank[i], tournament[i], t_round[i], result[i])
        c.execute("INSERT INTO chess_data VALUES " + row)
        # Save (commit) the changes
    except:
        pass
db_conn.commit()
# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
db_conn.close()

print('Done!')
sys.exit(0)

# X = list(zip(nameW, nameB, day, month, year, whiteRank, blackRank, tournament, t_round, result))




del date
del day
del month
del year
print('discard all the shit')
Y = [int(2 * x - 1) for x in result]
X = pd.DataFrame(X)
Y = pd.DataFrame(Y)

n = len(nameW)
k1 = m.floor(0.2 * n)
k2 = m.floor(0.8 * n)
X_train = X[:k1]
Y_train = Y[:k1]
X_CV = X[k1:k2]
Y_CV = Y[k1:k2]
X_test = X[k2:]
Y_test = Y[k2:]

del X
del Y





# X_train = pd.concat([X_train,white_name_dummies_train,black_name_dummies_train,tournament_dummies_train], axis=1)

# with open('df_X_train', 'wb') as f:
# 	for i in range(n):
# 		temp = X_train[i] + white_name_dummies_train[i] + black_name_dummies_train[i] + tournament_dummies_train[i]
# 		pickle.dump(temp, f, pickle.HIGHEST_PROTOCOL)
# 	# pickle.dump(X_train, f, pickle.HIGHEST_PROTOCOL)
# with open('df_Y_train', 'wb') as g:
# 	pickle.dump(Y_train, g, pickle.HIGHEST_PROTOCOL)

# ################## PART 2: ESTIMATION ######################

# print("Commencing estimation...")

# #Max_Depth = list(range(2,10)) #The maximum depth of the tree
# n_est = 100 #Number of estimations.
# #Max_Feat = [None,"log2","sqrt",0.2,0.5,0.8] #The number of features to consider when looking for the best split
# Max_Depth = [5]
# Max_Feat = [0.8]
# for max_feat in Max_Feat:
# 	for maxdepth in Max_Depth:
# 		clf = RandomForestClassifier(n_jobs=-1, n_estimators=n_est, max_depth = maxdepth, max_features = max_feat)
# 		print("Fitting the values...")
# 		clf = clf.fit(X_train, Y_train)
# 		y_hat_train = clf.predict(X_train)
# 		y_hat_cv = clf.predict(X_CV)
# 		y_hat_test = clf.predict(X_test)
# 		accuracy_train = sum([x[0] == x[1] for x in list(zip(y_hat_train,Y_train))])/len(y_hat_train)
# 		accuracy_CV = sum([x[0] == x[1] for x in list(zip(y_hat_cv,Y_CV))])/len(y_hat_cv)
# 		accuracy_test = sum([x[0] == x[1] for x in list(zip(y_hat_test,Y_test))])/len(y_hat_test)
# 		print(maxdepth,max_feat)
# 		print("The accuracy of your prediction on the training set is %f" %accuracy_train)
# 		print("The accuracy of your prediction on CV set is %f" %accuracy_CV)
# 		print("The accuracy of your prediction on the test set is %f" %accuracy_test)
# 		with open('results.csv', 'a', newline='') as csvfile:
# 			spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
# 			spamwriter.writerow([max_feat,maxdepth])
# 			spamwriter.writerow([accuracy_train,accuracy_CV,accuracy_test])
