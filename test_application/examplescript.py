from sequential_loading.data_storage import SQLStorage

my_storage = SQLStorage("postgresql://username@localhost:5432/teststorage", createdb=True)