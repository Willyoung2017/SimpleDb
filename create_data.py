import sys
n = int(sys.argv[1])
m = int(sys.argv[2])
file_path = "some_data_file_"+str(n)+"_"+str(m)+".txt"
print("Creating "+str(n)+" rows,"+str(m)+" cols to "+file_path+".")

with open(file_path,"w") as f:
    for i in range(n):
        for j in range(m):
            if j == m-1:
                f.write(str(i)+"\n")
            else:
                f.write(str(i)+",")

print("Finished.")
