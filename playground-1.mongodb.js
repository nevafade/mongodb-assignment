/* global use, db */
// MongoDB Playground
// To disable this template go to Settings | MongoDB | Use Default Template For Playground.
// Make sure you are connected to enable completions and to be able to run a playground.
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.
// The result of the last command run in a playground is shown on the results panel.
// By default the first 20 documents will be returned with a cursor.
// Use 'console.log()' to print to the debug output.
// For more documentation on playgrounds please refer to
// https://www.mongodb.com/docs/mongodb-vscode/playgrounds/

use('enrolment')
db.grouped_data.createIndex({Name: 1 })
db.grouped_data.createIndex({Grade:1})
db.grouped_data.createIndex({CreditPointAttempted: 1 })
db.grouped_data.createIndex({CreditPointPassed: 1 })
db.grouped_data.createIndex({'course_details.Level': 1 })
db.units.createIndex({Code:1})
db.self_rel.createIndex({type:1 })
db.self_rel.createIndex({for:1 })
db.self_rel.createIndex({uc:1 })
db.self_rel.createIndex({refCode:1 })
db.self_rel.createIndex({for: 1,uc:1 })

use('enrolment')
db.grouped_data.aggregate([
  {$sort: { Name:1 }},
    {$match: {CreditPointAttempted:{$ne:0}}},
    {$addFields: {
      numerator_wam: {$multiply: ['$Mark','$CreditPointAttempted']}
    }},
    {$group: 
        {_id: '$Name', 
        total: { $sum: '$Mark' },
        total_numerator_wam: { $sum: '$numerator_wam'},
        totalCPAttempted: { $sum: '$CreditPointAttempted'},
        totalCPpassed: { $sum: '$CreditPointPassed'}
        }
    },{
      $sort: {
        _id: 1
      }
    },
    {$project:
        { name: "$_id", 
            cp_passed: "$totalCPpassed", 
            wam: {  
                    $divide: ["$total_numerator_wam","$totalCPAttempted"]       
                },
            _id: 0
        }
    }
    
]).explain('executionStats')

//answer2-new-table
use('enrolment')
db.self_rel.aggregate({
  $match: {
    type: 'Requirement'
  }
},{
  $group:{
    _id:{
      Code: '$for',
      uc: '$uc'
    }
  }
},{
  $project:{
    Code: '$_id.Code',
    uc: '$_id.uc',
    _id: 0
  }
},{
  $addFields:{
    Requirement: {
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
    prerequisite_number: {$sum:'$Requirement'}
  }
},{
  $group:{
    _id:'$prerequisite_number',
    countofunit: {$sum:1}
  }
}
    ).explain('executionStats')


use('enrolment')
db.units.aggregate({ $addFields: { 
                        regex_or: { $regexFindAll: {input:'$Prerequisites', regex: / OR | or /} } ,
                        regex_and: { $regexFindAll: {input:'$Prerequisites', regex: / AND | and /} } 
                } 
            }, 
            {$addFields: {
                prerequisite_number:{ $cond: { 
                                        if: { $eq: [{$size:"$regex_and"},0]}, 
                                        then: { $cond:{ 
                                            if: { $eq: [{$size:"$regex_or"},0]}, 
                                            then: { $cond: [{ $eq: ['$Prerequisites',""]},0,1]  }, 
                                            else:1 }  
                                    },
                                        else:{$add:[1,{$size:"$regex_and"}]}
                                        } 
                                    }
            }}, {$group: {_id: '$prerequisite_number', units_arr: {$addToSet: "$Code"} }},
            {$project: { prerequisite_number:"$_id", unit_number: { $size: "$units_arr" } } }
            
    ).explain('executionStats')


use('enrolment')
db.grouped_data.aggregate([ 
         {$match: {'course_details.Level':{$exists:true}}},
         {$group: { _id:{
                            Name: "$Name", 
                            Level: "$course_details.Level"}, 
                            cp: { $sum: '$CreditPointAttempted' } } 
        }, { $addFields: {
          Name: "$_id.Name",
          Level: "$_id.Level"
        } }, { $unset: "_id" }, 
        { $group: {
          _id: '$Name',
          total_cp: {
            $push: { Level: "$Level", cp: "$cp" }
                }
            } 
        }, { $project: {
          Name: "$_id",
          _id: 0,
          total_cp: "$total_cp"
        } }, { $sort: { Name: 1 } }
]).explain('executionStats')

//introducing a new table
use('enrolment')
db.units.aggregate({})
use('enrolment')
db.units.aggregate(
            {
                $addFields:
                  {
                    alteredList: { $replaceAll: { input: "$Prohibitions", find: ' OR ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' AND ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' or ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' and ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' OF ', replacement: "," } }
                  }
              },
        { $addFields: {
            PrerequisitesArr: { $split: ['$alteredList',','] }
            }
        }, 
        {$unwind: { path: '$PrerequisitesArr'}},
        {$project: 
            { refCode: 
                { $trim: 
                    { input: '$PrerequisitesArr', chars: ' ()' }
                }, 
                type: 'Prohibition',
                for: '$Code',
                uc: 'none',
                _id: 0
            }
        },{
          $out: 'collection_prohibition'
        } 
)

use('enrolment')
db.units.aggregate(
            {
                $addFields:
                  {
                    alteredList: { $replaceAll: { input: "$Prerequisites", find: ' OR ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' AND ', replacement: "][" } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' or ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' and ', replacement: "][" } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' OF ', replacement: "," } }
                  }
              },
        { $addFields: {
            rArr: { $split: ['$alteredList',']['] }
            }
        }, 
        {$unwind: { path: '$rArr'}},
        {$project: 
            { uc: 
                { $trim: 
                    { input: '$rArr', chars: ' ()' }
                }, 
                type: 'Requirement',
                for: '$Code'
            }
        },{
          $addFields: {
            ucArr: { $split: ['$uc',','] }
          }
        }, {
          $unwind: '$ucArr'
        }, {
          $project: {
            refCode: '$ucArr',
            type: 1,
            for: 1,
            uc: 1,
            _id: 0
          }
        },{
          $out: 'collection_prerequisite'
        }
)
use('enrolment')
db.collection_prerequisite.aggregate([
  {
    $unionWith: { coll: "collection_prohibition" } // Merge with the second collection
  },
  {
    $out: "self_rel" // Specify the target collection name using $out
  }
]);
use('enrolment')
db.self_rel.find({ type:'Requirement'})
use('enrolment')
db.self_rel.find({ type:'Prohibition'})
use('enrolment')
db.self_rel.find()

//A2-Answer4-new table
use('enrolment')
db.self_rel.aggregate({
  $match: {
    refCode: {
      $ne:""
    }
  }
},{
  $lookup:{
    from: 'units',
    localField: 'refCode',
    foreignField: 'Code',
    as: 'course_details'
  }
},{
  $match: {
    course_details: {
      $size: 0
    }
  }
},{
  $group:{ _id:null, no_entry_units: {
    $addToSet : '$refCode'
  }}
},{
  $addFields:{
    unit_count:{ $size: '$no_entry_units'}
  }
}).explain('executionStats')



//A2-Answer4
use('enrolment')
db.units.aggregate([ 
    { $addFields: { pandp: { $concat: [ '$Prerequisites', ',' , '$Prohibitions'] }  }  },
    {
    $addFields:
      {
        alteredList: { $replaceAll: { input: "$pandp", find: ' OR ', replacement: "," } }
      }
  },
  {
    $set:
      {
        alteredList: { $replaceAll: { input: "$alteredList", find: ' AND ', replacement: "," } }
      }
  },
  {
    $set:
      {
        alteredList: { $replaceAll: { input: "$alteredList", find: ' or ', replacement: "," } }
      }
  },
  {
    $set:
      {
        alteredList: { $replaceAll: { input: "$alteredList", find: ' and ', replacement: "," } }
      }
  },
  {
    $set:
      {
        alteredList: { $replaceAll: { input: "$alteredList", find: ' OF ', replacement: "," } }
      }
  },
{ $addFields: {
PrerequisitesArr: { $split: ['$alteredList',','] }
}
},
{$unwind: { path: '$PrerequisitesArr'}} ,
{$project: 
    { Prerequisite_uc: 
        { $trim: 
            { input: '$PrerequisitesArr', chars: ' ().' }
        } 
    }
},
{ $match: { Prerequisite_uc: { $ne: ""} } },
{ $group: { _id:'$Prerequisite_uc'}},
{
    $lookup: {
      from: 'units',
      let: { uc: '$_id' },
      pipeline: [{ $group: {
        _id: null,
        listUC: {
          $push: '$Code'
              }
          } 
      },{
        $addFields: { show: { $not: { $in: ['$$uc','$listUC'] } } } 
      },{
        $project: { listUC:0 }
      }],
      as: 'result'
    }
},{
    $project: {
        Prerequisite_uc: '$_id',
      show: { $arrayElemAt: ["$result.show", 0] }
    }
},{
    $match: { show: true }
}, {
    $group: {
      _id: '$show',
      no_entry_units: {
        $push: '$Prerequisite_uc'
      }
    }
},{
    $addFields: {
        unit_count: { $size: '$no_entry_units' }
    }
}

]).explain('executionStats')


use('enrolment')
db.grouped_data.find({ CreditPointPassed: 0})

//answer5-with-new-table
var sname = 'Fayer'
var ucode = 'COMP5338'
use('enrolment')
db.self_rel.aggregate({
  $match:{
    for: ucode,
    type: 'Prohibition'
  }
},{
  $lookup:{
    from:'grouped_data',
    let: { prohibitated_code: '$refCode' },
    pipeline:[{
      $match: {
        Name: sname,
        CreditPointPassed: {$ne:0}
      }
    },{
      $group:{
        _id: '$Name',
        attempts: {$push:'$UnitCode'}
      }
    },{
      $addFields: {
        passed: {$in:['$$prohibitated_code','$attempts']}
      }
    }],
    as: 'student_info'
  }
},{
  $addFields:{
    passed: { $arrayElemAt: ['$student_info.passed',0] }
  }
},{
  $project: {
    student_info: 0
  }
},{
  $group:{
    _id:null,
    hasAttemptedProhibitedUnits: {$max:'$passed'}
  }
},{
  $project:{
    _id: 0,
    satisfy_prohibition: {$not: '$hasAttemptedProhibitedUnits'}
  }
}
).explain('executionStats')

//A2-Answer5-all-correct-with-schema
var sname = 'Alice'
var ucode = 'COMP2017'
use('enrolment')
db.grouped_data.aggregate(
     { $match: { Name: sname }},
     { $group: { _id:'$Name', cAttempts: { $push: '$UnitCode' } } },
     { $lookup: {
        from: 'units',
        let: { cAttempts: '$cAttempts' },
        pipeline:[
            { $match: { Code: ucode } },
            {
                $addFields:
                  {
                    alteredList: { $replaceAll: { input: "$Prohibitions", find: ' OR ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' AND ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' or ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' and ', replacement: "," } }
                  }
              },
              {
                $set:
                  {
                    alteredList: { $replaceAll: { input: "$alteredList", find: ' OF ', replacement: "," } }
                  }
              },
        { $addFields: {
            PrerequisitesArr: { $split: ['$alteredList',','] }
            }
        }, 
        {$unwind: { path: '$PrerequisitesArr'}},
        {$addFields: 
            { Prerequisite_uc: 
                { $trim: 
                    { input: '$PrerequisitesArr', chars: ' ()' }
                } 
            }
        } , { $match: { $expr: { $in: ['$Prerequisite_uc','$$cAttempts'] } } }
        ],
        as: 'shouldNotbe0'
     } }, { $addFields: {satisfy_proibition: { $eq: [{ $size: "$shouldNotbe0"},0] } } }
).explain('executionStats')


//A2-Answer6-with-new-table
var sname = 'Alice'
var ucode = 'COMP3888'
use('enrolment')
db.self_rel.aggregate({
  $match:{
    for: ucode,
    type: 'Requirement'
  }
},{
  $group:{
    _id:'$uc',
    unit_codes: {$push: '$refCode'}
  }
},{
  $lookup:{
    from: 'grouped_data',
    let: {requirements:'$unit_codes'},
    pipeline:[{
      $match:{
        Name: sname,
        CreditPointPassed: {$ne:0}
      }
    },{
      $addFields: {
        satisfied: {$in: ['$UnitCode','$$requirements']}
      }
    },{
      $group:{
        _id:null,
        requirement_satisfied: {$max:'$satisfied'}
      }
    }],
    as: 'student_info'
  }
},{
  $project:{
    requirement_satisfied: {$arrayElemAt: ['$student_info.requirement_satisfied',0]}
  }
},{
  $group:{
    _id:null,
    satisfy_prerequisite: {$min: '$requirement_satisfied'}
  }
}).explain('executionStats')


//A2-Answer6-all-correct-with-schema
var sname = 'Alice'
var ucode = 'COMP3888'
use('enrolment')
db.grouped_data.aggregate(
    { $match: {  Grade: { $not: { $regex: /FA|DC/ }} }},
    { $match: { Name: sname }},
    { $group: { _id: "Name", cAttempts: { $push: '$UnitCode' } } },
    { $lookup: {
       from: 'units',
       let: { cAttempts: '$cAttempts' },
       pipeline:[
           { $match: { Code: ucode } },
           {
               $addFields:
                 {
                   alteredList: { $replaceAll: { input: "$Prerequisites", find: ' OR ', replacement: "," } }
                 }
             },
             {
               $set:
                 {
                   alteredList: { $replaceAll: { input: "$alteredList", find: ' AND ', replacement: "][" } }
                 }
             },
             {
               $set:
                 {
                   alteredList: { $replaceAll: { input: "$alteredList", find: ' or ', replacement: "," } }
                 }
             },
             {
               $set:
                 {
                   alteredList: { $replaceAll: { input: "$alteredList", find: ' and ', replacement: "][" } }
                 }
             },
             {
               $set:
                 {
                   alteredList: { $replaceAll: { input: "$alteredList", find: ' OF ', replacement: "," } }
                 }
             },
       { $addFields: {
           PrerequisitesArr: { $split: ['$alteredList',']['] }
           }
       }, 
       {$unwind: { path: '$PrerequisitesArr'}},
       {$addFields: { requirements: { $map: {
                                        input: { $split: ['$PrerequisitesArr', ","] },
                                        as: "element",
                                        in: { $trim: { input: "$$element", chars: ' ()' } }
                                        } 
                                    } 
                    } 
        }, { $addFields: { unitsSatifyingRequirement: { 
            $setIntersection: ['$requirements','$$cAttempts']
        } } }, 
        { $addFields: 
            { requiremntSatisfied: {
                 $ne: [{ $size: '$unitsSatifyingRequirement' },0] }  
                } 
            }, {
                $group: { _id:null, checks: { $push: '$requiremntSatisfied' }}
            }
       ],
       as: 'checks'
    } }, { $project: {
        _id: 0,
        satisfy_prereqisites_check: '$checks.checks'
    }},{ $addFields: {
      satisfy_prereqisites: { 
        $reduce: {
            input: { $arrayElemAt: ["$satisfy_prereqisites_check", 0] },
            initialValue: true, 
            in: { $and: ["$$value", "$$this"] }  
          }
      }
    } }
).explain('executionStats')



// Select the database to use.
use('mongodbVSCodePlaygroundDB');

// Insert a few documents into the sales collection.
db.getCollection('sales').insertMany([
  { 'item': 'abc', 'price': 10, 'quantity': 2, 'date': new Date('2014-03-01T08:00:00Z') },
  { 'item': 'jkl', 'price': 20, 'quantity': 1, 'date': new Date('2014-03-01T09:00:00Z') },
  { 'item': 'xyz', 'price': 5, 'quantity': 10, 'date': new Date('2014-03-15T09:00:00Z') },
  { 'item': 'xyz', 'price': 5, 'quantity': 20, 'date': new Date('2014-04-04T11:21:39.736Z') },
  { 'item': 'abc', 'price': 10, 'quantity': 10, 'date': new Date('2014-04-04T21:23:13.331Z') },
  { 'item': 'def', 'price': 7.5, 'quantity': 5, 'date': new Date('2015-06-04T05:08:13Z') },
  { 'item': 'def', 'price': 7.5, 'quantity': 10, 'date': new Date('2015-09-10T08:43:00Z') },
  { 'item': 'abc', 'price': 10, 'quantity': 5, 'date': new Date('2016-02-06T20:20:13Z') },
]);

// Run a find command to view items sold on April 4th, 2014.
const salesOnApril4th = db.getCollection('sales').find({
  date: { $gte: new Date('2014-04-04'), $lt: new Date('2014-04-05') }
}).count();

// Print a message to the output window.
console.log(`${salesOnApril4th} sales occurred in 2014.`);

// Here we run an aggregation and open a cursor to the results.
// Use '.toArray()' to exhaust the cursor to return the whole result set.
// You can use '.hasNext()/.next()' to iterate through the cursor page by page.
db.getCollection('sales').aggregate([
  // Find all of the sales that occurred in 2014.
  { $match: { date: { $gte: new Date('2014-01-01'), $lt: new Date('2015-01-01') } } },
  // Group the total sales for each product.
  { $group: { _id: '$item', totalSaleAmount: { $sum: { $multiply: [ '$price', '$quantity' ] } } } }
]);
