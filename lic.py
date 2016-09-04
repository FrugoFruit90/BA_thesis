import math as m
import csv
from sklearn.ensemble import RandomForestClassifier
import pickle
import time
import numpy as np
import matplotlib.pyplot as plt
# ################## PART 1: LOADING DATA ######################

print("Data loading initiated.")
with open('df_X', 'rb') as f:
    Z = pickle.load(f)

# print(Z.columns.values)
Y = 2 * Z[['result']]
Y = np.ravel(Y)
X = Z[['whiterank', 'blackrank', 'result_in_t_w', 'result_in_t_b', 'games_t_w', 'games_t_b', 'games_c_w', 'games_c_b',
       'games_6m_w', 'games_6m_b', 'result_6m_w', 'result_6m_b']]
# use X = Z[['whiterank', 'blackrank']] to make predictions using only ELO rating
del Z

n = len(X)
k1 = m.floor(0.6 * n)
k2 = m.floor(0.8 * n)
X_train = X[:k1]
Y_train = Y[:k1]
X_CV = X[k1:k2]
Y_CV = Y[k1:k2]
X_test = X[k2:]
Y_test = Y[k2:]

del X
del Y

# ################## PART 2: ESTIMATION ######################

print("Commencing estimation with elo only...")

Max_Depth = list(range(2, 15))  # The maximum depth of the tree
n_est = 100  # Number of estimations.
Max_Feat = [2, 4, 6, 8, 10, 12]  # The number of features to consider when looking for the best split

for max_feat in Max_Feat:
    for maxdepth in Max_Depth:
        start = time.time()
        print("Creating the classifier for Max_Depth = ", maxdepth, " and Max_Feat = ", max_feat)
        clf = RandomForestClassifier(n_jobs=6, n_estimators=n_est, max_depth=maxdepth, max_features=max_feat)
        print("Fitting the values...")
        clf = clf.fit(X_train, Y_train)
        print("Calculating and fitting feature importances...")
        importances = clf.feature_importances_
        std = np.std([tree.feature_importances_ for tree in clf.estimators_],
                     axis=0)
        indices = np.argsort(importances)[::-1]

        # Plot the feature importances of the estimator.
        # This is computationally expensive so only do this once the best parameter values are found.
        plt.figure()
        plt.title("Feature importances")
        plt.bar(range(X_train.shape[1]), importances[indices],
                color="r", yerr=std[indices], align="center")
        plt.xticks(range(X_train.shape[1]), indices)
        plt.xlim([-1, X_train.shape[1]])
        plt.show()
        plt.savefig('feature_importances.png')

        print("Predicting results...")
        y_hat_train = clf.predict(X_train)
        y_hat_cv = clf.predict(X_CV)
        y_hat_test = clf.predict(X_test)
        accuracy_train = sum([x[0] == x[1] for x in list(zip(y_hat_train, Y_train))]) / len(y_hat_train)
        accuracy_CV = sum([x[0] == x[1] for x in list(zip(y_hat_cv, Y_CV))]) / len(y_hat_cv)
        accuracy_test = sum([x[0] == x[1] for x in list(zip(y_hat_test, Y_test))]) / len(y_hat_test)
        print(maxdepth, max_feat)
        print("The accuracy of your prediction on the training set is %f" % accuracy_train)
        print("The accuracy of your prediction on CV set is %f" % accuracy_CV)
        print("The accuracy of your prediction on the test set is %f" % accuracy_test)

        # Saving results to the file
        with open('results.csv', 'a', newline='') as csvfile:
        # use: with open('results_elo.csv', 'a', newline='') as csvfile: for the other case
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(["Accuracy for Max_Depth = ", maxdepth, "and Max_Feat = ", max_feat])
            writer.writerow([accuracy_train, accuracy_CV, accuracy_test])
            writer.writerow(["Vector of feature importances is the following: ", clf.feature_importances_])
        end = time.time()
        print(end - start)
