gcc -I `pg_config --includedir` -L `pg_config --libdir` $1 -lpq -lm -pthread -o copy_fast
