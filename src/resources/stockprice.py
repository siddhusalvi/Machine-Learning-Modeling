import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score,cross_validate,train_test_split
from sklearn.preprocessing import StandardScaler,Normalizer, RobustScaler
from sklearn.metrics import *
import sys
import pickle
import os
 
 
data_path = sys.argv[1]
model_path = sys.argv[2]

df = pd.read_csv(data_path,thousands=',')

df.head()

train_set = df.drop('date',axis =1)

train_set.head()

y_train = train_set['open']
train_set.drop('open',axis=1,inplace=True)

x_train,x_test,y_train,y_test = train_test_split(train_set,y_train,test_size=0.2)


linear_reg = LinearRegression(normalize=True)
linear_reg.fit(x_train,y_train)


pred = linear_reg.predict(x_test)

error_mae = mean_absolute_error(pred,y_test)
error_rmse = mean_squared_error(pred,y_test)
accuracy = r2_score(pred,y_test)*100

print("error rmse:",error_rmse,"error mae:",error_mae,"R2 scrore accuracy: ",accuracy)

path = model_path

file = open(path,'wb')
pickle.dump(linear_reg,file)
file.close()

try :
    sys.stdout.write(model_path)
except Exception as e:
    print("path not returned as ",e)

