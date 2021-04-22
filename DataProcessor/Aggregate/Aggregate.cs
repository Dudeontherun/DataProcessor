using DataProcessor.Interfaces;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DataProcessor.Aggregate
{
	[Serializable]
	public class Aggregate : IAggregate
	{
		protected Guid _id;
		Guid IAggregate.Id { get => this._id; set => this._id = value; }

		public string Function { get; private set; }

		public string[] OldColumnNames { get; set; }
		public string NewColumnName { get; set; }

		//HACK: use dynamic to invoke!
		private dynamic _function;

		public Aggregate(string function, string columnName, Guid identifer)
		{
			this._id = identifer;
			this.OldColumnNames = new string[0] { };
			this.NewColumnName = columnName;
			this.Function = function;
		}

		public Aggregate(string function, string oldColumName, string newColumnName, Guid identifier)
		{
			this._id = identifier;
			this.OldColumnNames = new string[1] { oldColumName };
			this.NewColumnName = newColumnName;
			this.Function = function;
		}

		public Aggregate(string function, string[] oldColumnName, string newColumnName, Guid identifier)
		{
			this._id = identifier;
			this.OldColumnNames = oldColumnName;
			this.NewColumnName = newColumnName;
			this.Function = function;
		}

		public object ProcessColumn(params object[] item)
		{
			if(item == null || item.Length != OldColumnNames.Length) { throw new Exception("Not enough parameters provided"); }
			
			object result = null;
			try 
			{
				switch(item.Length)
				{
					case 0: { result = this._function.Invoke(); break; }
					case 1: { result = this._function.Invoke(item[0]); break; }
					case 2: { result = this._function.Invoke(item[0], item[1]); break; }
					case 3: { result = this._function.Invoke(item[0], item[1], item[2]); break; }
					case 4: { result = this._function.Invoke(item[0], item[1], item[2], item[3]); break; }
					case 5: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4]); break; }
					case 6: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5]); break; }
					case 7: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6]); break; }
					case 8: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7]); break; }
					case 9: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8]); break; }
					case 10: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9]); break; }
					case 11: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10]); break; }
					case 12: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11]); break; }
					case 13: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12]); break; }
					case 14: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13]); break; }
					case 15: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13], item[14]); break; }
					case 16: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13], item[14], item[15]); break; }
					case 17: { result = this._function.Invoke(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], item[11], item[12], item[13], item[14], item[15], item[16]); break; }
				}
			}
			catch (Exception e) { throw new Exception("Inovking function failed", e); }

			return result;
		}


		private object _initLock = new object();
		public bool Init()
		{
			lock (_initLock)
			{
				if (this._function != null) { return true; }

				//TODO: Find CSharp parser.
				string lambda = this.Function;

				var sb = new StringBuilder("");
				var refPaths = new[] {
				typeof(System.Object).GetTypeInfo().Assembly.Location,
				typeof(Console).GetTypeInfo().Assembly.Location,
				Path.Combine(Path.GetDirectoryName(typeof(System.Runtime.GCSettings).GetTypeInfo().Assembly.Location), "System.Runtime.dll")
			};

				var assemblyNames = new[]
				{
				"System",
			};

				foreach (string s in assemblyNames)
				{
					sb.AppendFormat("using {0};{1}", s, Environment.NewLine);
				}

				//sb.AppendLine("using System.Linq;");
				string tempNameSpace = "abc";
				string tempClassName = "myClass";
				List<string> args = this.OldColumnNames.Select(i => "object").ToList();
				args.Add("object>");
				string functionRetType = "Func<" + String.Join(",", args.ToArray());
				//string functionRetType = "Func<object, object>";
				string methodName = "Create";

				sb.AppendFormat("namespace {0} {{ {1}", tempNameSpace, Environment.NewLine); //Start Namespace
				sb.AppendFormat("public class {0} {{ {1}", tempClassName, Environment.NewLine); //Start Namespace
				sb.AppendFormat("public {0} {1}() {{", functionRetType, methodName);//Start Function

				//TODO: figure out a way to not allow hackers to escapte this...

				sb.AppendFormat("{0} qwerty = {1};", functionRetType, lambda);

				sb.AppendLine("return qwerty;");  //End Function
				sb.AppendLine("}");  //End Function
				sb.AppendLine("}");  //End Class
				sb.AppendLine("}");  //End Namespace


				//Compile
				string code = sb.ToString();
				var tree = CSharpSyntaxTree.ParseText(code);
				MetadataReference[] references = refPaths.Select(r => MetadataReference.CreateFromFile(r)).ToArray();
				var options = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary);
				var compile = CSharpCompilation.Create("boo", new[] { tree }, references, options);

				byte[] assemblyBytes;
				using (var memStream = new System.IO.MemoryStream())
				{
					var result = compile.Emit(memStream);

					if (result.Success == false)
					{
						//TODO: Figure out what to say.  Give code or show errors?  Maybe both?


						return false;
					}

					memStream.Seek(0, System.IO.SeekOrigin.Begin);
					assemblyBytes = memStream.ToArray();
				}

				Assembly assembly = Assembly.Load(assemblyBytes);

				//Create and instance of the temp class
				Object temp = assembly.CreateInstance(string.Format("{0}.{1}", tempNameSpace, tempClassName));
				Type t = temp.GetType();
				MethodInfo mi = t.GetMethod(methodName);
				//Call the method, and get the resulting object.
				object ret = mi.Invoke(temp, null);

				//TODO: Convert Ret into a function that I can invoke.
				this._function = ret;

			}

			return true;
		}
	}
}
