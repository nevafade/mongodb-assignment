// Replace the MongoDB statement "db.students.aggregate([{$match: {Name: name}}, {$match: {UnitCode: code}}])" with your solution.

var res = db.self_rel.aggregate({
  $match:{
    for: ucode,
    type: 'Prohibition' // filters for assigned code, filter considers only prohibitions not requirements 
  }
},{
  $lookup:{ // stage to identify units that are attempted
    from:'grouped_data',
    let: { prohibitated_code: '$refCode' },
    pipeline:[{
      $match: {
        Name: sname,
        CreditPointPassed: {$ne:0} // filtering units that were fail, filtering for assigned student name
      }
    },{
      $group:{
        _id: '$Name',
        attempts: {$push:'$UnitCode'} // creating array of courses student passed
      }
    },{
      $addFields: {
        passed: {$in:['$$prohibitated_code','$attempts']} //indicates if unit is passed
      }
    }],
    as: 'student_info'
  }
},{
  $addFields:{
    passed: { $arrayElemAt: ['$student_info.passed',0] } //changing shape
  }
},{
  $project: {
    student_info: 0 // removing irrelevent fields
  }
},{
  $group:{
    _id:null,
    hasAttemptedProhibitedUnits: {$max:'$passed'} // value will false only if all vlaues for passes is false
  }
},{
  $project:{
    _id:0,
    satisfy_prohibition: {$not: '$hasAttemptedProhibitedUnits'} // if student has not Attempted Prohibited units then prohibition is satisfied
  }
}
)
