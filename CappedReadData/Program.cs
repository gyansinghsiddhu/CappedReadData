using System;
using System.IO;
using System.Net;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using MongoDB.Driver.Linq;


namespace CappedReadData
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("==========Read Data From CAPPED COLLECTION=============");
            var dbClient = new MongoClient("mongodb://myUserAdmin:1qazXSW%40@168.138.189.225:27727/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false");
            IMongoDatabase db = dbClient.GetDatabase("TestingMongoDB");
            //var CappedPromotion = db.GetCollection<BsonDocument>("CappedPromotion");

            IMongoCollection<BsonDocument> TailCollectionTatget = db.GetCollection<BsonDocument>("CappedPromotion");
            TailCollection(TailCollectionTatget);

            #region
            // var documents = CappedPromotion.Find(new BsonDocument()).ToList();
            // string PromoID ="";
            // foreach (BsonDocument Capped_doc in documents)
            // {

            //     PromoID = Capped_doc["PromotionID"].ToString();
            //     Console.WriteLine(PromoID);

            // }

            //Console.WriteLine("==========END INSERTION , Please PRESS ENTER =============");
            // Console.ReadLine();
            #endregion
        }




        private static void TailCollection(IMongoCollection<BsonDocument> collection)
        {
            // Set lastInsertDate to the smallest value possible
            BsonValue lastInsertDate = BsonMinKey.Value;

            var options = new FindOptions<BsonDocument>
            {
                // Our cursor is a tailable cursor and informs the server to await
                CursorType = CursorType.TailableAwait
            };

            // Initially, we don't have a filter. An empty BsonDocument matches everything.
            BsonDocument filter = new BsonDocument();

            // NOTE: This loops forever. It would be prudent to provide some form of 
            // an escape condition based on your needs; e.g. the user presses a key.
            while (true)
            {
                // Start the cursor and wait for the initial response
                using (var cursor = collection.FindSync(filter, options))
                {
                    foreach (var document in cursor.ToEnumerable())
                    {
                        // Set the last value we saw 
                        lastInsertDate = document["InsertDate"];

                        // Write the document to the console.
                        Console.WriteLine(document.ToString());
                    }
                }

                // The tailable cursor died so loop through and restart it
                // Now, we want documents that are strictly greater than the last value we saw
                filter = new BsonDocument("$gt", new BsonDocument("InsertDate", lastInsertDate));
            }
        }


        //private static void TailCollection(IMongoCollection<BsonDocument> collection)
        //{
        //    // Set lastInsertDate to the smallest value possible
        //    BsonValue lastInsertId = BsonMinKey.Value;

        //    var options = new FindOptions<BsonDocument>
        //    {
        //        // Our cursor is a tailable cursor and informs the server to await
        //        CursorType = CursorType.TailableAwait
        //    };

        //    // Initially, we don't have a filter. An empty BsonDocument matches everything.
        //    BsonDocument filter = new BsonDocument();

        //    // NOTE: This loops forever. It would be prudent to provide some form of 
        //    // an escape condition based on your needs; e.g. the user presses a key.
        //    while (true)
        //    {
        //        // Start the cursor and wait for the initial response
        //        using (var cursor = collection.FindSync(filter, options))
        //        {
        //            foreach (var document in cursor.ToEnumerable())
        //            {
        //                // Set the last value we saw 
        //                lastInsertId = document["_id"];

        //                // Write the document to the console.
        //                Console.WriteLine(document.ToString());
        //            }
        //        }

        //        // The tailable cursor died so loop through and restart it
        //        // Now, we want documents that are strictly greater than the last value we saw
        //        filter = new BsonDocument("gt", new BsonDocument("_id", lastInsertId));
        //    }
        //}

    }
}
