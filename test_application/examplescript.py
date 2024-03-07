from sequential_loading.data_storage import SQLStorage

my_storage = SQLStorage("postgresql://bodszab@localhost:5432/teststorage", createdb=True)