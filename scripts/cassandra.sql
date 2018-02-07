create keyspace stopbot with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table stopbot.bots (ip ascii primary key, period timestamp, reason text);