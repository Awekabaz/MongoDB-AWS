import utils.formatHelper

def formatDate(dataframe):
    cList = ['SomeColumn1', 'SomeColumn2', 'SomeColumn3', 'SomeColumn4', 'SomeColumn5', 'SomeColumn6', 'SomeColumn7', 'SomeColumn8']
    for column in dataframe.columns:
        if column in cList:
            dataframe[column] = dataframe[column].astype(str).apply(lambda row: utils.formatHelper.formatDateHelper(row))
            dataframe[column] = dataframe[column].astype(str)

def formatNumber(dataframe) -> int:
    nList = ['SomeColumn1', 'SomeColumn2', 'SomeColumn3', 'SomeColumn4']
    for column in dataframe.columns:
        if column in nList:
            dataframe[column] = dataframe[column].astype(str).apply(utils.formatHelper.formatNumberHelper)
