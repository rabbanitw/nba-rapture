import pickle

# Read a pickle file
with open('Data/LOG.pkl', 'rb') as file:
    data = pickle.load(file)

print(data)