import psycopg2
conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
cur = conn.cursor()
cur.execute("""

SELECT * FROM accounts;
""")
conn.commit()

data = open("test_data1.txt",'r')
print(data)
for row in data:
    cur.execute("""
    INSERT INTO accounts VALUES(%d, %s, %f)
    """, 1, row, 1.90)
result = cur.fetchall()
print (result)

