conn = new Mongo();

db = conn.getDB("enrolment");

print("** Workload 1 result **");
load("w1.js");
while (res.hasNext())
{
    printjson(res.next());
}
print("");

print("** Workload 2 result **");
load("w2.js");
while (res.hasNext())
{
    printjson(res.next());
}
print("");

print("** Workload 3 result **");
load("w3.js");
while (res.hasNext())
{
    printjson(res.next());
}
print("");

print("** Workload 4 result **");
load("w4.js");
while (res.hasNext())
{
    printjson(res.next());
}
print("");

print("The first four aggregation workloads have finished execution.");
