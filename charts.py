import pickle

DEBUG = False

if DEBUG == True:
    with open('df_X_mini', 'rb') as f:
        X = pickle.load(f)
else:
    with open('df_X', 'rb') as f:
        X = pickle.load(f)
# print(len(X))
# print(X['namew'])
result_in_t_w = list()
result_in_t_b = list()
games_in_t_w = list()
games_in_t_b = list()

for i, row in X.iterrows():
    t_games_w = 0
    t_games_b = 0
    for j, row2 in X[:i].iterrows():
        if (X['namew'][j] == X['namew'][i] or X['nameb'][j] == X['namew'][i])\
                and X['tournament'][i] == X['tournament'][j]:
            t_games_w += 1
        if (X['nameb'][j] == X['namew'][i] or X['nameb'][j] == X['namew'][i])\
                and X['tournament'][i] == X['tournament'][j]:
            t_games_b += 1
    games_in_t_w.append(t_games_w)
    games_in_t_b.append(t_games_b)