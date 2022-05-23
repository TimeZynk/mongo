# 1) Create a database folder
mkdir mongodb

# 2) Start a server:
mongod --replSet rs0 --dbpath ./mongodb --port 27017

# 3) In a new terminal window, start the replica set:
mongo --eval "rs.initiate({ \"_id\": \"rs0\", members: [ { \"_id\": 0, host: \"localhost:27017\" } ]}, { force: true })"

# 4) Test.
