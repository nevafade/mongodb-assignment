// Replace the MongoDB statement "db.students.aggregate([{$match: {Name: uname}}, {$match: {UnitCode: code}}])" with your solution.
var res = db.self_rel.aggregate({
  $match:{
    for: ucode,
    type: 'Requirement' // filters for assigned code, filter considers only requirements not prohibitions
  }
},{
  $group:{
    _id:'$uc',
    unit_codes: {$push: '$refCode'} // Group refCodes that share a AND relationship to each other
  }
},{
  $lookup:{ // stage to identify units that are attempted
    from: 'grouped_data',
    let: {requirements:'$unit_codes'},
    pipeline:[{
      $match:{
        Name: sname,
        CreditPointPassed: {$ne: 0} // filtering units that were fail, filtering for assigned student name
      }
    },{
      $addFields: {
        satisfied: {$in: ['$UnitCode','$$requirements']}  //indicates if attempted unit satisfied each OR expression on prerequisites
      }
    },{
      $group:{
        _id:null,
        requirement_satisfied: {$max:'$satisfied'} // if any of the units satiisfies the OR expression, OR exprssion is marked as satisfied:true
      }
    }],
    as: 'student_info'
  }
},{
  $project:{
    requirement_satisfied: {$arrayElemAt: ['$student_info.requirement_satisfied',0]} //reshaping
  }
},{
  $group:{
    _id:null,
    satisfy_prerequisite: {$min: '$requirement_satisfied'} // becomes true only if all values are true ( All requiremnts need to be statified)
  }
},{
  $project:{ //removing irrelevent fields
    _id:0,
    satisfy_prerequisite: 1
  }
})