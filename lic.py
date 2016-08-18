import math as m
import csv
from datetime import datetime as dt
import psycopg2
import sys
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import pickle

with open('df_X', 'rb') as f:
    X = pickle.load(f)

# for index, row in X.iterrows():
#     print(row)

# Y = [int(2 * x - 1) for x in result]
# X = pd.DataFrame(X)
# Y = pd.DataFrame(Y)
#
# n = len(X)
# k1 = m.floor(0.6 * n)
# k2 = m.floor(0.8 * n)
# X_train = X[:k1]
# Y_train = Y[:k1]
# X_CV = X[k1:k2]
# Y_CV = Y[k1:k2]
# X_test = X[k2:]
# Y_test = Y[k2:]
#
# del X
# del Y

# X_train = pd.concat([X_train,white_name_dummies_train,black_name_dummies_train,tournament_dummies_train], axis=1)00,00,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,0,

# ################## PART 2: ESTIMATION ######################

# print("Commencing estimation...")
#
# # Max_Depth = list(range(2,10)) #The maximum depth of the tree
# n_est = 500  # Number of estimations.
# # Max_Feat = [None,"log2","sqrt",0.2,0.5,0.8] #The number of features to consider when looking for the best split
# Max_Depth = [5]
# Max_Feat = [0.8]
# for max_feat in Max_Feat:
#     for maxdepth in Max_Depth:
#         clf = RandomForestClassifier(n_jobs=-1, n_estimators=n_est, max_depth=maxdepth, max_features=max_feat)
#         print("Fitting the values...")
#         clf = clf.fit(X_train, Y_train)
#         y_hat_train = clf.predict(X_train)
#         y_hat_cv = clf.predict(X_CV)
#         y_hat_test = clf.predict(X_test)
#         accuracy_train = sum([x[0] == x[1] for x in list(zip(y_hat_train, Y_train))]) / len(y_hat_train)
#         accuracy_CV = sum([x[0] == x[1] for x in list(zip(y_hat_cv, Y_CV))]) / len(y_hat_cv)
#         accuracy_test = sum([x[0] == x[1] for x in list(zip(y_hat_test, Y_test))]) / len(y_hat_test)
#         print(maxdepth, max_feat)
#         print("The accuracy of your prediction on the training set is %f" % accuracy_train)
#         print("The accuracy of your prediction on CV set is %f" % accuracy_CV)
#         print("The accuracy of your prediction on the test set is %f" % accuracy_test)
#         with open('results.csv', 'a', newline='') as csvfile:
#             spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#             spamwriter.writerow([max_feat, maxdepth])
#             spamwriter.writerow([accuracy_train, accuracy_CV, accuracy_test])
