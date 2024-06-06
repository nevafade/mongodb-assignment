// Replace the MongoDB statement "db.units.aggregate([{$match: {code: "COMP5045"}}])" with your solution.
var res = db.self_rel.aggregate({
    $match: {
      type: 'Requirement' // filtering out prohibitions from table
    }
  },{
    $group:{
      _id:{
        Code: '$for', // creating array for units that have OR between them
        uc: '$uc'
      }
    }
  },{
    $project:{
      Code: '$_id.Code', //removing irrelevant information
      uc: '$_id.uc',
      _id: 0
    }
  },{
    $addFields:{
      Requirement: { // assigning zero for units that have a empty string for prerequites
        $cond:{
          if: {$eq:['$uc','']},
          then: 0,
          else: 1
        }
      }
    }
  },{
    $group:{
      _id: '$Code',
      prerequisite_number: {$sum:'$Requirement'} // aggerating number of prerequsite units for a unit
    }
  },{
    $group:{
      _id:'$prerequisite_number',
      unit_number: {$sum:1} // counting number of units
    }
  },{
    $project:{ // removing irrelevant field
      prerequisite_number: '$_id',
      unit_number: 1
    }
  }
      )
