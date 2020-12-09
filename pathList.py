import os
import pandas as pd

# Getting the current work directory (cwd)
#thisdir = os.getcwd()
thisdir="C:/Users/wichu/Documents/GitHub"

# r=root, d=directories, f = files
path_list=[]
for r, d, f in os.walk(thisdir):
    for file in f:
        if file.endswith(".csv"):
            #print(os.path.join(r, file))
            path_list.append(os.path.join(r, file))

print(thisdir)