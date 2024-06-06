const fs = require("fs");


conn = new Mongo();

db = conn.getDB("enrolment");
// Read the JSON file and parse it into an array of objects
const data = JSON.parse(fs.readFileSync("w5_inputs.json"));

print("** Workload 5 result **");
for (let obj of data) {
    // Extract the name and code values from the object
    var sname = obj.sname;
    var ucode = obj.ucode;
    print("Student name: " + sname)
    print("Unit code: " + ucode)
    load("w5.js");
    print ("Query Result:")
    while (res.hasNext())
    {
        printjson(res.next());
    }
    print("==================");
}
print("Aggregation workload five has finished execution.");
