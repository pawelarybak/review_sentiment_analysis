import sys
import textwrap
import pandas as pd
import numpy as np

def get_mean_err(arr): 
    return np.mean(abs(arr['label'] - arr['prediction']))

def get_max_err(arr):
    return max(abs(arr['label'] - arr['prediction']))

def get_acurracy(arr):
    return len(arr[arr['label'] == arr['prediction']])/len(arr)


def main():
    if not sys.argv[1]:
        print('Please pass file name')
    data = pd.read_csv(sys.argv[1])
    print(textwrap.dedent(f'''
    Acurracy: {get_acurracy(data)}
    Mean error: {get_mean_err(data)}
    Max error: {get_max_err(data)}
    {(get_acurracy(data), get_mean_err(data), get_max_err(data))}
    '''))

if __name__ == "__main__":
    main()