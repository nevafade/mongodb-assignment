// Replace the MongoDB statement "db.units.aggregate([{$match: {code: "COMP5045"}}])" with your solution.
var res = db.grouped_data.aggregate([
    {$match: {CreditPointAttempted:{$ne:0}}},
    {$addFields: {
      numerator_wam: {$multiply: ['$Mark','$CreditPointAttempted']} //calculation for wam numerator
    }},
    {$group: 
        {_id: '$Name', 
        total_numerator_wam: { $sum: '$numerator_wam'}, // calculating numerator for wam
        totalCPAttempted: { $sum: '$CreditPointAttempted'}, // calculating denomenator for wam
        totalCPpassed: { $sum: '$CreditPointPassed'} // calculating cp_passed
        }
    },{
      $sort: { // sorting based on name
        _id: 1
      }
    },
    {$project: // showing relevent calculations
        { name: "$_id", 
            cp_passed: "$totalCPpassed", 
            wam: {  
                    $divide: ["$total_numerator_wam","$totalCPAttempted"]       
                },
            _id: 0
        }
    }
    
])
