import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import preprocessing

# open the file 
df = pd.read_csv('teleCust1000t.csv') 
print(df.head())

#Let’s see how many of each class is in our data set
print(df['custcat'].value_counts())

df.hist(column='income', bins=50)
#lets define feture sets : 

df.columns
X = df[['region', 'tenure','age', 'marital', 'address', 'income', 'ed', 'employ','retire', 'gender', 'reside']] .values  #.astype(float)
X[0:5]

y = df['custcat'].values
y[0:5]

#normalizing data 

X = preprocessing.StandardScaler().fit(X).transform(X.astype(float))
X[0:5]

#test trin split 

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.2, random_state=4)
print ('Train set:', X_train.shape,  y_train.shape)
print ('Test set:', X_test.shape,  y_test.shape)

#import k nearest neibour library 
from sklearn.neighbors import KNeighborsClassifier

#let's train 

k = 5
#Train Model and Predict  
neigh = KNeighborsClassifier(n_neighbors = k).fit(X_train,y_train)
print(neigh)

#now we can use this model for more preduction 

yhat = neigh.predict(X_test)
print(yhat[0:5])

#now evaluate accuracy
from sklearn import metrics
print("Train set Accuracy: ", metrics.accuracy_score(y_train, neigh.predict(X_train)))
print("Test set Accuracy: ", metrics.accuracy_score(y_test, yhat))

# Plot the accuracy for different values of k
Ks = 10
mean_acc = np.zeros((Ks-1))
std_acc = np.zeros((Ks-1))

for n in range(1, Ks):
    # Train Model and Predict
    neigh = KNeighborsClassifier(n_neighbors = n).fit(X_train, y_train)
    yhat = neigh.predict(X_test)
    mean_acc[n-1] = metrics.accuracy_score(y_test, yhat)
    std_acc[n-1] = np.std(yhat == y_test) / np.sqrt(yhat.shape[0])

plt.figure(figsize=(8,6))
plt.plot(range(1, Ks), mean_acc, 'g')
plt.fill_between(range(1, Ks), mean_acc - 1 * std_acc, mean_acc + 1 * std_acc, alpha=0.10)
plt.legend(('Accuracy ', '+/- 1xstd'))
plt.ylabel('Accuracy')
plt.xlabel('Number of Neighbors (K)')
plt.title('KNN Accuracy for Different K Values')
plt.tight_layout()
plt.show()
