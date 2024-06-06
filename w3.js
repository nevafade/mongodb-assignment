// Replace the MongoDB statement "db.units.aggregate([{$match: {code: "COMP5045"}}])" with your solution.
var res = db.grouped_data.aggregate([ 
  {$match: {'course_details.Level':{$exists:true}}}, // filter for units not present in the unit table
  {$group: { _id:{ // creating relevant groups
                     Name: "$Name", 
                     Level: "$course_details.Level"}, 
                     cp: { $sum: '$CreditPointAttempted' } } 
 }, { $addFields: { //changing shape
   Name: "$_id.Name",
   Level: "$_id.Level"
 } }, { $unset: "_id" }, // removing irrelevant field
 { $group: { //creating relevent groups
   _id: '$Name',
   total_cp: {
     $push: { Level: "$Level", cp: "$cp" }
         }
     } 
 }, { $project: { // removing irrelevant field
   Name: "$_id",
   _id: 0,
   total_cp: "$total_cp"
 } }, { $sort: { Name: 1 } } //sorting based on name
])