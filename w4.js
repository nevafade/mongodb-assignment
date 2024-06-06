// Replace the MongoDB statement "db.units.aggregate([{$match: {code: "COMP5045"}}])" with your solution.
var res = db.self_rel.aggregate({
    $match: {
      refCode: { // filtering for units that have no prereqisuiets
        $ne:""
      }
    }
  },{
    $lookup:{ //identifies which refcodes are present in the units database
      from: 'units',
      localField: 'refCode',
      foreignField: 'Code',
      as: 'course_details'
    }
  },{
    $match: { //filters units not present in units
      course_details: {
        $size: 0
      }
    }
  },{
    $group:{ _id:null, no_entry_units: { // creates array
      $addToSet : '$refCode'
    }}
  },{
    $addFields:{ // adds count
      unit_count:{ $size: '$no_entry_units'}
    }
  })