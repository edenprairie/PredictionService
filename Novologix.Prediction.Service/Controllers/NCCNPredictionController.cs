using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Microsoft.ML;
using System.Data.SqlClient;

namespace Novologix.Prediction.Service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class NCCNPredictionController : ControllerBase
    {

        public class data
        {
            public List<Compendiumdata> items { get; set; }
            public string stage { get; set; }
            public string bioMarker { get; set; }
            public string indication { get; set; }
        }

        public class Compendiumdata
        {
            public int CompendiumID { get; set; }
            public string RecommendedUse { get; set; }
            public int Score { get; set; }
            public List<string> tokens { get; set; }
        }

        
        [HttpPost]
        public async Task<ActionResult<string>> Post()
        {
            string postedString = await Request.GetRawBodyStringAsync();
            data Data = JsonConvert.DeserializeObject<data>(postedString);
            //SqlDAL dal = new SqlDAL(@"server = PAZ1NLXSQLDW1V\SQL12DED1; Integrated Security=true");

            //List<string> biomarkerSynonyms = dal.GetListObject<string>(@"Select AlsoKnownAs from NlxReference.dbo.PredictionSynonyms Where CateGory = @Category and candidate = @candidate",
            //    new SqlParameter("Category", "NCCN.BioMarker"),
            //    new SqlParameter("candidate",Data.bioMarker)
            //);

            //List<string> stageSynonyms = dal.GetListObject<string>(@"Select AlsoKnownAs from NlxReference.dbo.PredictionSynonyms Where CateGory = @Category and candidate = @candidate",
            //    new SqlParameter("Category", "NCCN.CancerStage"),
            //    new SqlParameter("candidate", Data.stage)
            //);

            //List<string> indicationSynonyms = dal.GetListObject<string>(@"Select AlsoKnownAs from NlxReference.dbo.PredictionSynonyms Where CateGory = @Category and candidate = @candidate",
            //    new SqlParameter("Category", "NCCN.TreatmentSettings"),
            //    new SqlParameter("candidate", Data.indication)
            //);

            var reUses = string.Join("****", Data.items.Select(c => c.RecommendedUse.ToLower()));
            var t = await GetTokens(reUses);
            var sentenceTokens = t.Split(new string[] { "/********************************************************/" }, StringSplitOptions.RemoveEmptyEntries);

            int i = 0;
            foreach (var item in Data.items)
            {
                int biomarkerMatched = 0;
                int stageMatched = 0;
                int indicationMatched = 0;

                
                var tks = sentenceTokens[i].Split(new string[] { Environment.NewLine.ToString() }, StringSplitOptions.RemoveEmptyEntries);
                i++;
                
                if(tks.Any(c=> c.Contains(Data.bioMarker.ToLower())))
                {
                    //see if the parameters are in as it is
                    biomarkerMatched = 2;
                }
                else
                {
                    //look at the synonyms
                    //foreach (string bm in biomarkerSynonyms)
                    //{
                    //    if (tks.Any(c => c.Contains(bm.ToLower())))
                    //    {
                    //        biomarkerMatched = 2;
                    //        break;
                    //    }
                    //}
                }

                if (tks.Any(c => c.Contains(Data.stage.ToLower())))
                {
                    //see if the parameters are in as it is
                    stageMatched = 2;
                }
                else
                {
                    //look at the synonyms
                    //foreach (string bm in stageSynonyms)
                    //{
                    //    if (tks.Any(c => c.Contains(bm.ToLower())))
                    //    {
                    //        stageMatched = 2;
                    //        break;
                    //    }
                    //}
                }

                if (tks.Any(c => c.Contains(Data.indication.ToLower())))
                {
                    //see if the parameters are in as it is
                    indicationMatched = 2;
                }
                else
                {
                    //look at the synonyms
                    //foreach (string bm in indicationSynonyms)
                    //{
                    //    if (tks.Any(c => c.Contains(bm.ToLower())))
                    //    {
                    //        indicationMatched = 2;
                    //        break;
                    //    }
                    //}
                }

                float sum = biomarkerMatched + stageMatched + indicationMatched;
                float score = (sum / 6) * 100;
                int Score = Convert.ToInt32(Math.Ceiling(score));

                item.Score = Score;
            }
            Data.items = Data.items.OrderByDescending(c => c.Score).ToList();
            return JsonConvert.SerializeObject(Data);
        }

        private string GetTokensUsingML(string input)
        {
            var mlContext = new MLContext();
            var emptySamples = new List<TextData>();
            var emptyDataView = mlContext.Data.LoadFromEnumerable(emptySamples);
            var textPipeline = mlContext.Transforms.Text.TokenizeIntoWords("Words","Text", separators: new[] { ' ' });
            var textTransformer = textPipeline.Fit(emptyDataView);
            var predictionEngine = mlContext.Model.CreatePredictionEngine<TextData,TransformedTextData>(textTransformer);

            var data = new TextData()
            {
                Text = "ML.NET's TokenizeIntoWords API " +
                "splits text/string into words using the list of characters " +
                "provided as separators."
            };

            var prediction = predictionEngine.Predict(data);

            return string.Join(Environment.NewLine, prediction.Words);

        }

        private class TextData
        {
            public string Text { get; set; }
        }

        private class TransformedTextData : TextData
        {
            public string[] Words { get; set; }
        }

        public async Task<string> GetTokens(string searchString)
        {
            ProcessStartInfo start = new ProcessStartInfo();
            start.FileName = @"C:\Python27\python.exe";
            start.Arguments = string.Format("\"{0}\" \"{1}\"", @"Tagger.py", searchString);
            start.UseShellExecute = false;// Do not use OS shell
            start.CreateNoWindow = true; // We don't need new window
            start.RedirectStandardOutput = true;// Any output, generated by application will be redirected back
            start.RedirectStandardError = true; // Any error in standard output will be redirected back (for example exceptions)
            using (Process process = Process.Start(start))
            {
                using (StreamReader reader = process.StandardOutput)
                {
                    string stderr = process.StandardError.ReadToEnd(); // Here are the exceptions from our Python script
                    string result = await reader.ReadToEndAsync(); // Here is the result of StdOut(for example: print "test")
                    return result;
                }
            }
        }
    }
}