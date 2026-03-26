import pandas as pd

raw_df = pd.read_csv("data/train2-25.csv")

useful_columns = ['RedFighter', 'BlueFighter', 'RedOdds', 'BlueOdds', 'RedExpectedValue', 'BlueExpectedValue', 
    'Date', 'Winner', 'TitleBout', 'WeightClass', 'Gender', 'NumberOfRounds', 
    'BlueCurrentLoseStreak', 'BlueCurrentWinStreak', 'BlueLongestWinStreak', 'BlueLosses', 'BlueTotalRoundsFought', 'BlueTotalTitleBouts', 
    'BlueWinsByKO', 'BlueWinsBySubmission', 'BlueWins', 'BlueStance', 'BlueHeightCms', 'BlueReachCms', 'BlueAge', 'BMatchWCRank', 
    'RedCurrentLoseStreak', 'RedCurrentWinStreak', 'RedLongestWinStreak', 'RedLosses', 'RedTotalRoundsFought', 'RedTotalTitleBouts', 
    'RedWinsByKO', 'RedWinsBySubmission', 'RedWins', 'RedStance', 'RedHeightCms', 'RedReachCms', 'RedAge', 'RMatchWCRank', 
    'BetterRank', 'Finish', 'FinishRound']

df = raw_df[useful_columns]

print(df.dtypes)