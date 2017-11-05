using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;

namespace Lucene.Unity {

    public class Lucene3D {

        public Lucene3D(string name = "index") {
            // TODO: Guard against bad names.
            indexDirectory = new DirectoryInfo(Path.Combine(Application.persistentDataPath, name));
        }

        public void DefineIndexTerm<T>(string name, Func<T, string> indexer, IndexOptions options) {
            if(indexer == null) {
                throw new ArgumentNullException(nameof(indexer));
            }
            var type = typeof(T);
            if(!indexers.ContainsKey(type)) {
                indexers.Add(type, new TypeDefinition());
            }
            var typeDefinition = indexers[type];
            var indexDefinition = new IndexDefinition { Name = name, Indexer = GenericiseLambda(indexer), Options = options };
            if(options == IndexOptions.PrimaryKey) {
                if(typeDefinition.PrimaryKey != null) {
                    throw new ArgumentException("Option IndexOptions.PrimaryKey can only be specified for one index.", nameof(options));
                }
                typeDefinition.PrimaryKey = indexDefinition;
            }
            typeDefinition.Indexers.Add(indexDefinition);
        }

        public void Index<T>(T item) {
            if(item == null) {
                throw new ArgumentNullException(nameof(item));
            }
            IndexInternal(new T[] { item }, processYields: false);
        }

        public void Index<T>(IEnumerable<T> items) {
            IndexInternal(items, processYields: false);
        }

        public IEnumerator IndexCoroutine<T>(IEnumerable<T> items, int timeSlice = 13) {
            yield return IndexInternal<T>(items, timeSlice, true);
        }

        private IEnumerator IndexInternal<T>(IEnumerable<T> items, int timeSlice = 13, bool processYields = true) {
            if(items == null) {
                throw new ArgumentNullException(nameof(items));
            }
            var type = typeof(T);
            if(!indexers.ContainsKey(type)) {
                throw new ArgumentOutOfRangeException(nameof(items), "At least one index must be defined using DefineIndexTerm for a type before it can be indexed.");
            }
            var keyIndexer = indexers[type].PrimaryKey;
            var definitions = indexers[type].Indexers;
            var analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var count = 0;
            var notNullItems = items.Where(e => e != null);
            var total = notNullItems.Count();
            OnProgress("Indexing", 0, total);
            var directory = FSDirectory.Open(IndexDirectory);
            var create = !IndexReader.IndexExists(directory);
            UnityEngine.Debug.Log($"Need to create index? {create}");
            using(var writer = new IndexWriter(directory, analyzer, create, IndexWriter.MaxFieldLength.LIMITED)) {
                foreach(var item in notNullItems) {
                    var doc = new Document();
                    Term term = null;
                    if(keyIndexer != null) {
                        term = new Term(keyIndexer.Name, keyIndexer.Indexer(item));
                    }
                    foreach(var definition in definitions) {
                        try {
                            var value = definition.Indexer(item);
                            var field = new Field(definition.Name, value, definition.StoreType(), definition.IndexType());
                            doc.Add(field);
                        }
                        catch {
                            // The indexer passed in may have failed, what do we do with this exception? Just swallow it?
                        }
                    }
                    if(term != null) {
                        writer.UpdateDocument(term, doc);
                    }
                    else {
                        writer.AddDocument(doc);
                    }
                    if(stopwatch.ElapsedMilliseconds >= timeSlice) {
                        OnProgress("Indexing", count, total);
                        if(processYields) {
                            yield return null;
                        }
                        stopwatch.Restart();
                    }
                    ++count;
                }
                OnProgress("Indexing", count, total);
                if(processYields) {
                    yield return null;
                }
                OnProgress("Optimizing", 0, 1);
                if(processYields) {
                    yield return null;
                }
                writer.Optimize();
                writer.Commit();
                OnProgress("Optimizing", 1, 1);
            }
        }

        public IEnumerable<Document> Search(string expression, int maxResults = 100) {
            if(expression == null) {
                throw new ArgumentNullException(nameof(expression));
            }
            try {
                //Debug.Log($"Directory: {IndexDirectory.FullName}");
                using(var indexReader = IndexReader.Open(FSDirectory.Open(IndexDirectory), true)) {
                    using(var indexSearcher = new IndexSearcher(indexReader)) {
                        using(var analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30)) {
                            var parser = new QueryParser(Lucene.Net.Util.Version.LUCENE_30, "headline", analyzer);
                            var query = parser.Parse(expression);
                            var hits = indexSearcher.Search(query, null, maxResults).ScoreDocs;
                            var docs = hits.Select(e => indexSearcher.Doc(e.Doc)).ToList(); // Need ToList as indexSearcher will be disposed...
                            return docs;
                        }
                    }
                }
            }
            catch {
                return null;
            }
        }

        public event EventHandler<LuceneProgressEventArgs> Progress;
        private LuceneProgressEventArgs progressEventArgs = new LuceneProgressEventArgs();
        private Stopwatch progressStopwatch = new Stopwatch();
        private void OnProgress(string title, int count, int total) {
            if(count == 0) {
                progressStopwatch.Restart();
            }
            if(Progress != null) {
                progressEventArgs.Title = title;
                progressEventArgs.Count = count;
                progressEventArgs.Total = total;
                progressEventArgs.Duration = (int)progressStopwatch.ElapsedMilliseconds;
                Progress(this, progressEventArgs);
            }
        }

        private bool InvalidSearchTerm(string text) {
            return text.StartsWith("~") || string.IsNullOrWhiteSpace(text);
        }

        private Func<object, string> GenericiseLambda<T>(Func<T, string> func) {
            if(func == null) {
                return null;
            }
            else {
                return new Func<object, string>(o => func((T)o));
            }
        }

        private DirectoryInfo IndexDirectory {
            get {
                if(indexDirectory == null) {
                    indexDirectory = new DirectoryInfo(Path.Combine(Application.persistentDataPath, "index"));
                }
                return indexDirectory;
            }
        }
        private DirectoryInfo indexDirectory;

        private Dictionary<Type, TypeDefinition> indexers = new Dictionary<Type, TypeDefinition>();

        private class TypeDefinition {
            public IndexDefinition PrimaryKey { get; set; }
            public List<IndexDefinition> Indexers { get; } = new List<IndexDefinition>();
        }

        private class IndexDefinition {
            public string Name { get; set; }
            public Func<object, string> Indexer { get; set; }
            public IndexOptions Options { get; set; }
            public Field.Store StoreType() {
                return Options == IndexOptions.IndexTerms ? Field.Store.NO : Field.Store.YES;
            }
            public Field.Index IndexType() {
                switch(Options) {
                    case IndexOptions.IndexTermsAndStore:
                        return Field.Index.ANALYZED;
                    case IndexOptions.IndexTerms:
                        return Field.Index.ANALYZED;
                    case IndexOptions.IndexTextAndStore:
                        return Field.Index.NOT_ANALYZED;
                    case IndexOptions.StoreOnly:
                        return Field.Index.NO;
                    default:
                        return Field.Index.ANALYZED;
                }
            }
        }

    }

}
