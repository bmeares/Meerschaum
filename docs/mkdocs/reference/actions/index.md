# ⏯️ Actions

<link rel="stylesheet" type="text/css" href="/assets/css/grid.css" />

<div class="grid-container center">

  <div class="grid-child">

    <p>Meerschaum actions (commands) are designed with a verb-noun syntax and recognize both singular and plural nouns (e.g. <code>sync pipe</code> is the same as <code>sync pipes</code>).</p>

  </div>

  <div class="grid-child">

    <div class="admonition tip">
      <p>To see all of the available commands, run <code>help</code> or <code>show actions</code>.</p>

      <p>To get help for specific actions, add the flag <code>-h</code> to any command, or preface the command with <code>help</code> when inside the shell (e.g. <code>help show pipes</code>).</p>
    </div>

  </div>

</div>

Commands may be run directly on the command line with `mrsm`, or in an interactive shell, which you can start with the command `mrsm`.

Below are all of the Meerschaum commands and descriptions.

<style type="text/css">
.tg  {border:none;border-collapse:collapse;border-spacing:0;}
.tg td{border-style:solid;border-width:0px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;
  padding:10px 5px;word-break:normal;}
.tg th{border-style:solid;border-width:0px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-fcjd{background-color:#fce6da;font-family:"Lucida Console", Monaco, monospace !important;font-size:small;text-align:left;
  vertical-align:top}
.tg .tg-alq7{background-color:#ffdcdc;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:small;text-align:left;vertical-align:top}
.tg .tg-lni3{background-color:#deecff;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;text-align:left;
  vertical-align:top}
.tg .tg-q1tx{background-color:#deecff;font-family:"Lucida Console", Monaco, monospace !important;font-size:small;text-align:left;
  vertical-align:top}
.tg .tg-v2cg{background-color:#deecff;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:small;text-align:left;vertical-align:top}
.tg .tg-vu0g{background-color:#fce6da;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:medium;text-align:center;vertical-align:middle}
.tg .tg-pdrq{background-color:#fce6da;border-color:inherit;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;
  text-align:left;vertical-align:top}
.tg .tg-e079{background-color:#deecff;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;text-align:left;
  vertical-align:middle}
.tg .tg-tijg{background-color:#cbe2ff;font-family:"Lucida Console", Monaco, monospace !important;text-align:left;vertical-align:top}
.tg .tg-5eak{background-color:#add0ff;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;font-weight:bold;
  text-align:center;vertical-align:middle}
.tg .tg-8evx{background-color:#fce6da;font-family:"Lucida Console", Monaco, monospace !important;text-align:left;vertical-align:top}
.tg .tg-cl16{background-color:#ffdcdc;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;text-align:center;
  vertical-align:middle}
.tg .tg-2c8s{background-color:#ffdcdc;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;text-align:left;
  vertical-align:top}
.tg .tg-6qkm{background-color:#ffdcdc;font-family:"Lucida Console", Monaco, monospace !important;font-size:small;text-align:left;
  vertical-align:top}
.tg .tg-j46h{background-color:#deecff;font-family:"Lucida Console", Monaco, monospace !important;text-align:left;vertical-align:top}
.tg .tg-9rd5{background-color:#ffdcdc;border-color:inherit;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;
  text-align:left;vertical-align:top}
.tg .tg-lqts{background-color:#000000;border-color:inherit;color:#ffffff;font-family:Arial, Helvetica, sans-serif !important;
  font-size:large;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-7och{background-color:#000000;border-color:inherit;color:#ffffff;font-family:Arial, Helvetica, sans-serif !important;
  font-size:large;text-align:center;vertical-align:top}
.tg .tg-tv01{background-color:#ffb3b3;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;font-weight:bold;
  text-align:center;vertical-align:middle}
.tg .tg-9rv8{background-color:#fce6da;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:small;text-align:left;vertical-align:top}
.tg .tg-zwfs{background-color:#fce6da;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  text-align:left;vertical-align:top}
.tg .tg-kmkj{background-color:#ffdcdc;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:medium;text-align:center;vertical-align:middle}
.tg .tg-fiud{background-color:#ffdcdc;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  text-align:left;vertical-align:top}
.tg .tg-19ut{background-color:#add0ff;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:medium;font-weight:bold;text-align:center;vertical-align:middle}
.tg .tg-noas{background-color:#deecff;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  font-size:medium;text-align:center;vertical-align:middle}
.tg .tg-9tzq{background-color:#deecff;border-color:inherit;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;
  text-align:left;vertical-align:top}
.tg .tg-v284{background-color:#deecff;border-color:inherit;font-family:"Lucida Console", Monaco, monospace !important;
  text-align:left;vertical-align:top}
.tg .tg-yrlk{background-color:#cbe2ff;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;text-align:center;
  vertical-align:middle}
.tg .tg-3y4y{background-color:#cbe2ff;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;text-align:left;
  vertical-align:top}
.tg .tg-zjt4{background-color:#cbe2ff;font-family:"Lucida Console", Monaco, monospace !important;font-size:small;text-align:left;
  vertical-align:top}
.tg .tg-8hk5{background-color:#deecff;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;text-align:center;
  vertical-align:middle}
.tg .tg-dja1{background-color:#fce6da;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;text-align:center;
  vertical-align:middle}
.tg .tg-p1sh{background-color:#fce6da;font-family:Arial, Helvetica, sans-serif !important;font-size:medium;text-align:left;
  vertical-align:top}
.tg .tg-491y{background-color:#ffdcdc;font-family:"Lucida Console", Monaco, monospace !important;text-align:left;vertical-align:top}
.tg .tg-klia{background-color:#ffb3b3;font-family:"Lucida Console", Monaco, monospace !important;font-size:medium;text-align:center;
  vertical-align:middle}
</style>
<table class="tg">
<thead>
  <tr>
    <th class="tg-lqts">Action</th>
    <th class="tg-lqts">Sub-Actions</th>
    <th class="tg-lqts">Description</th>
    <th class="tg-7och" colspan="2"><span style="font-weight:bold">Flags</span></th>
    <th class="tg-7och"><span style="font-weight:bold">Examples</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-tv01" rowspan="2">api</td>
    <td class="tg-vu0g">{None}</td>
    <td class="tg-pdrq">Send commands to an API instance.<br><br><span style="font-weight:bold">Depreciated </span>― replaced by the instance command<br></td>
    <td class="tg-9rv8" colspan="2">• positional arguments<br>  May include the label to the API connector (e.g. 'main') followed by a list of commands.<br><br>• All other arguments are passed to the API instance.</td>
    <td class="tg-zwfs">api delete pipes -y<br><br>api main delete pipes -y<br></td>
  </tr>
  <tr>
    <td class="tg-kmkj">start, boot, init</td>
    <td class="tg-9rd5">Start the web API server.<br><br>Alias for the command start api.</td>
    <td class="tg-alq7">• -p, --port<br>  The port to bind to.<br>  Defaults to 8000.<br><br>• --host<br>  The host interface to bind to. Defaults to 0.0.0.0.<br><br>• -w, --workers<br>  How many threads to use for the server. Defaults to the number of cores.</td>
    <td class="tg-6qkm">• --no-dash, --nodash<br>  Do not start the web dashboard.<br>  <br>• --no-auth, --noauth<br>  Do not require authentication.<br>  <br>• --production, --gunicorn<br>  Run with web server via gunicorn.</td>
    <td class="tg-fiud">api start -p 8001 --no-dash</td>
  </tr>
  <tr>
    <td class="tg-19ut" rowspan="3">bootstrap</td>
    <td class="tg-noas">config</td>
    <td class="tg-9tzq">Delete and regenerate the default Meerschaum configuration.<br><br>Not used often; mostly meant for development<br></td>
    <td class="tg-v2cg" colspan="2">• -y, --yes<br>• -f, --force</td>
    <td class="tg-v284">bootstrap config -y<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">connectors</td>
    <td class="tg-3y4y">Launch the wizard for creating a new connector.<br><br>Useful when adding new database or API connections<br></td>
    <td class="tg-zjt4" colspan="2">• -y, --yes<br>• -f, --force</td>
    <td class="tg-tijg">bootstrap connectors</td>
  </tr>
  <tr>
    <td class="tg-8hk5">pipes</td>
    <td class="tg-e079">Launch the wizard for creating new pipes.<br><br>The recommended first action once you start up the <a href="/reference/stack/" target="_blank" rel="noopener noreferrer">stack</a>.<br><br>Here is more information about <a href="/reference/pipes/bootstrapping/" target="_blank" rel="noopener noreferrer">bootstrapping pipes</a></td>
    <td class="tg-q1tx">• -c, -C, --connector-keys<br>  The connector keys of the new pipes. Multiple values are allowed.<br><br>• -m, -M, --metric-keys<br>  The metric keys of the new pipes. Multiple values are allowed.<br>  <br>• -l, -L, --location-keys<br>  The location keys of the new pipes. The string 'None' will be parsed as None. Multiple values are allowed.</td>
    <td class="tg-q1tx">• -i, --instance, --mrsm-instance<br>  The connector keys string to the instance for the new pipes (defaults to 'sql:main').<br><br>• -y, --yes<br>• -f, --force<br>• --noask</td>
    <td class="tg-j46h">bootstrap pipes<br><br>bootstrap pipes -c sql:main sql:foo -m bar<br></td>
  </tr>
  <tr>
    <td class="tg-tv01" rowspan="2">clear</td>
    <td class="tg-dja1">{None}</td>
    <td class="tg-p1sh">Clear the screen.</td>
    <td class="tg-fcjd" colspan="2">None</td>
    <td class="tg-8evx">clear</td>
  </tr>
  <tr>
    <td class="tg-cl16">pipes</td>
    <td class="tg-2c8s">Delete rows (clear the pipe) within a given date range.</td>
    <td class="tg-6qkm">• --begin<br>  Only remove rows newer than this datetime.<br><br>• --end<br>  Only remove rows older than this datetime (not including end).</td>
    <td class="tg-6qkm">• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags<br>  Only select pipes with these tags<br></td>
    <td class="tg-491y">clear pipes -c plugin:foo \<br>  --begin 2022-01-01 --end 2022-02-01</td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="2">copy</td>
    <td class="tg-yrlk">connectors</td>
    <td class="tg-3y4y">Create new connectors from existing ones.<br><span style="font-weight:bold">NOTE:</span> Not implemented!<br></td>
    <td class="tg-zjt4" colspan="2">• -c, -C, --connector-keys</td>
    <td class="tg-tijg">copy connectors sql:main<br><br>copy connectors -c sql:main<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">pipes</td>
    <td class="tg-lni3">Create new pipes from existing ones.<br><br>Useful for migrating pipes between instances.<br></td>
    <td class="tg-q1tx">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-q1tx">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-j46h">copy pipes -i sql:main -m foo</td>
  </tr>
  <tr>
    <td class="tg-klia"><span style="font-weight:bold">debug</span></td>
    <td class="tg-dja1">{None}</td>
    <td class="tg-p1sh">Toggle debug mode.<br><br>Only available in the Meerschaum shell.<br></td>
    <td class="tg-fcjd" colspan="2">None</td>
    <td class="tg-8evx">debug</td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="6">delete</td>
    <td class="tg-yrlk">config</td>
    <td class="tg-3y4y">Delete configuration files.<br><br>May specify which config files to delete.</td>
    <td class="tg-zjt4" colspan="2">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-tijg">delete config<br><br>delete config plugins system<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">connectors</td>
    <td class="tg-lni3">Remove connectors from the configuration file.</td>
    <td class="tg-q1tx">• positional arguments<br>  The connector keys.<br><br><br>• -c, -C, --connector-keys</td>
    <td class="tg-q1tx">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-j46h">delete connectors sql:foo<br><br>delete connectors -c sql:foo<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">jobs</td>
    <td class="tg-3y4y">Remove background jobs.<br><br>If the jobs are running, it will ask to stop them first.<br></td>
    <td class="tg-zjt4">• positional arguments<br>  Names of jobs.<br></td>
    <td class="tg-zjt4">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-tijg">delete jobs -y<br><br><br>delete jobs sad_tram -y</td>
  </tr>
  <tr>
    <td class="tg-8hk5">pipes</td>
    <td class="tg-lni3">Drop pipes and remove their registration from the instance.</td>
    <td class="tg-q1tx">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-q1tx">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-j46h">delete pipes -c plugin:foo plugin:bar -y<br><br>delete pipes -i sql:mydb<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">plugins</td>
    <td class="tg-3y4y">Delete the plugins' registrations from the repository.</td>
    <td class="tg-zjt4">• positional arguments<br>  Names of plugins (without 'plugin:').<br><br>• -r, --repo, --repository<br>  The API instance keys of the<br>  plugins repository.</td>
    <td class="tg-zjt4">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-tijg">delete plugins foo bar -r api:mrsm</td>
  </tr>
  <tr>
    <td class="tg-8hk5">users</td>
    <td class="tg-lni3">Delete users from a Meerschaum instance.</td>
    <td class="tg-q1tx">• positional arguments<br>  List of usernames.<br><br>• -i, --instance, --mrsm-instance</td>
    <td class="tg-q1tx">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-j46h">delete users foo -i sql:db</td>
  </tr>
  <tr>
    <td class="tg-tv01" rowspan="2">drop</td>
    <td class="tg-cl16">pipes</td>
    <td class="tg-2c8s">Drop the pipes' tables but keep the registration.</td>
    <td class="tg-6qkm">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-6qkm">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-491y">drop pipes -m weather -y</td>
  </tr>
  <tr>
    <td class="tg-dja1">tables</td>
    <td class="tg-p1sh"><span style="font-weight:bold">NOTE:</span> Not implemented!</td>
    <td class="tg-fcjd" colspan="2">None</td>
    <td class="tg-8evx">drop tables foo</td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="3">edit</td>
    <td class="tg-yrlk">config</td>
    <td class="tg-3y4y">Open configuration files for editing.<br><br>May specify which files to open,<br>defaults to 'meerschaum'.<br></td>
    <td class="tg-zjt4" colspan="2">• positional arguments<br>  List of files to edit (without extensions).</td>
    <td class="tg-tijg">edit config<br><br>edit config plugins system shell</td>
  </tr>
  <tr>
    <td class="tg-8hk5">pipes {definition}</td>
    <td class="tg-lni3">Open YAML files to edit the parameters of pipes.<br><br>Add the word "definition" to open a SQL file<br>(SQL pipes only).<br></td>
    <td class="tg-q1tx">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-q1tx">• positional arguments<br>  "definition"</td>
    <td class="tg-j46h">edit pipes -c sql:foo<br><br>edit pipes definition -c sql:foo<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">users</td>
    <td class="tg-3y4y">Edit the attributes of users,<br>e.g. to make a user an administrator<br>or to change a password.</td>
    <td class="tg-zjt4">• positional arguments<br>  List of usernames.<br><br>• -i, --instance, --mrsm-instance</td>
    <td class="tg-zjt4">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug</td>
    <td class="tg-tijg">edit users bob -i sql:main<br><br>edit users alice bob<br></td>
  </tr>
  <tr>
    <td class="tg-tv01">help</td>
    <td class="tg-cl16">{None}<br></td>
    <td class="tg-2c8s">Print help text about actions.<br>Preface any command with 'help'.<br><br>Only available in the shell.<br>The flag -h works both in the shell<br>and on the command line.<br></td>
    <td class="tg-6qkm" colspan="2">• positional arguments<br>  The command to seek help for.</td>
    <td class="tg-491y">help delete pipes</td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="3">install</td>
    <td class="tg-8hk5">packages</td>
    <td class="tg-lni3">Install Python packages into<br>the Meerschaum virtual environment.</td>
    <td class="tg-q1tx">• positional arguments<br>  List of packages.<br><br>• -A, --sub-args<br>  Additional arguments to pass to pip.</td>
    <td class="tg-q1tx">• --debug</td>
    <td class="tg-j46h">install packages pandas numpy</td>
  </tr>
  <tr>
    <td class="tg-yrlk">plugins</td>
    <td class="tg-3y4y">Install Meerschaum plugins from a repository.</td>
    <td class="tg-zjt4">• positional arguments<br>  List of plugins.<br><br>• -r, --repo, --repository<br>  The keys to the Meerschaum API repository.</td>
    <td class="tg-zjt4">• -f, --force<br>• --debug</td>
    <td class="tg-tijg">install plugins noaa color<br><br>install plugins foo -r api:bar<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">required</td>
    <td class="tg-lni3">Install a plugins' required packages<br>into its virtual environment.</td>
    <td class="tg-q1tx">• positional arguments<br>  List of plugins.<br><br>• -r, --repo, --repository<br>  The keys to the Meerschaum API repository.</td>
    <td class="tg-q1tx">• -f, --force<br>• --debug</td>
    <td class="tg-j46h">install required noaa<br><br>install required foo -r api:bar<br></td>
  </tr>
  <tr>
    <td class="tg-tv01">login</td>
    <td class="tg-dja1">{None}</td>
    <td class="tg-p1sh">Log into a Meerschaum API instance<br>and save the credentials.</td>
    <td class="tg-fcjd">• positional arguments<br>  Connector keys for an API instance.<br><br>• -c, -C, --connector-keys<br></td>
    <td class="tg-fcjd">• -y, --yes<br>• --noask<br>• --debug</td>
    <td class="tg-8evx">login api:main api:foo api:bar</td>
  </tr>
  <tr>
    <td class="tg-5eak">python</td>
    <td class="tg-yrlk">{None}</td>
    <td class="tg-3y4y">Launch a Python REPL with Meerschaum already imported.<br><br>Python code may be include as a positional argument.<br></td>
    <td class="tg-zjt4" colspan="2">• positional arguments<br>  Python code to execute.<br><br>• --debug</td>
    <td class="tg-tijg">python<br><br>python 'print("foo")'<br></td>
  </tr>
  <tr>
    <td class="tg-tv01" rowspan="3">register</td>
    <td class="tg-cl16">pipes<br></td>
    <td class="tg-2c8s">Register new pipes on a Meerschaum instance.<br><br>Connector and metric keys are required.<br><br>Registering pipes from plugins with a <a href="/reference/plugins/writing-plugins/" target="_blank" rel="noopener noreferrer">register function</a><br>will set the pipes' parameters to the function's output.<br></td>
    <td class="tg-6qkm">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys</td>
    <td class="tg-6qkm">• --debug</td>
    <td class="tg-491y">register pipes -c plugin:noaa -m weather</td>
  </tr>
  <tr>
    <td class="tg-dja1">plugins</td>
    <td class="tg-p1sh">Publish a plugin onto a Meerschaum repository.</td>
    <td class="tg-fcjd">• positional arguments<br>  List of plugins.<br><br>• -r, --repo, --repository</td>
    <td class="tg-fcjd">• -y, --yes<br>• -f, --force<br>• --debug</td>
    <td class="tg-8evx">register plugins myplugin1 myplugin2<br><br>register plugins noaa -r api:foo<br></td>
  </tr>
  <tr>
    <td class="tg-cl16">users</td>
    <td class="tg-2c8s">Create new users on a Meerschaum instance.</td>
    <td class="tg-6qkm">• positional arguments<br>  List of users.<br><br>• -i, --instance, --mrsm-instance</td>
    <td class="tg-6qkm">• --debug</td>
    <td class="tg-491y">register users alice bob<br><br>register users bob -i sql:foo<br></td>
  </tr>
  <tr>
    <td class="tg-5eak">reload</td>
    <td class="tg-8hk5">{None}</td>
    <td class="tg-lni3">Reload the current Meerschaum process and plugins.<br><br>Useful when developing plugins.<br></td>
    <td class="tg-q1tx" colspan="2">• --debug</td>
    <td class="tg-j46h">reload</td>
  </tr>
  <tr>
    <td class="tg-tv01">setup</td>
    <td class="tg-dja1">plugins</td>
    <td class="tg-p1sh">Execute the setup function for Meerschaum plugins.</td>
    <td class="tg-fcjd" colspan="2">• positional arguments<br>  List of plugins.<br><br>• -r, --repo, --repository<br></td>
    <td class="tg-8evx">setup plugins noaa<br><br>setup plugins foo -r api:bar<br></td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="17">show</td>
    <td class="tg-yrlk">actions</td>
    <td class="tg-3y4y">Print a list of available Meerschaum actions.<br><br>If global Unicode settings are False, only use ASCII<br>(for almost all show commands).<br></td>
    <td class="tg-zjt4" colspan="2">• --nopretty</td>
    <td class="tg-tijg">show actions</td>
  </tr>
  <tr>
    <td class="tg-8hk5">arguments</td>
    <td class="tg-lni3">Print the provided command line arguments.</td>
    <td class="tg-q1tx" colspan="2">All arguments.<br>See mrsm -h for available flags.<br></td>
    <td class="tg-j46h">show arguments foo --begin 2022-01-01</td>
  </tr>
  <tr>
    <td class="tg-yrlk">columns</td>
    <td class="tg-3y4y">Print tables of pipes' columns.</td>
    <td class="tg-zjt4">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-zjt4">• --nopretty</td>
    <td class="tg-tijg">show columns -m foo</td>
  </tr>
  <tr>
    <td class="tg-8hk5">config</td>
    <td class="tg-lni3">Print the configuration dictionary.<br></td>
    <td class="tg-q1tx">• positional arguments<br>  Configuration keys to print.</td>
    <td class="tg-q1tx">• --nopretty</td>
    <td class="tg-j46h">show config<br><br>show config formatting emoji<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">connectors</td>
    <td class="tg-3y4y">Print the attributes for the registered connectors.</td>
    <td class="tg-zjt4">• positional arguments<br>  Keys for a specific connector.</td>
    <td class="tg-zjt4">• --nopretty</td>
    <td class="tg-tijg">show connectors<br><br>show connector sql:main<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">data</td>
    <td class="tg-lni3">Print previews of the contents of pipes.</td>
    <td class="tg-q1tx">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-q1tx">• --nopretty</td>
    <td class="tg-j46h">show data<br><br>show data -m weather<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">gui</td>
    <td class="tg-3y4y">Start the desktop terminal.<br><br>Alias for start gui.<br></td>
    <td class="tg-zjt4">• -p, --port<br>  The port for the webterm server.<br></td>
    <td class="tg-zjt4">• --debug</td>
    <td class="tg-tijg">show gui</td>
  </tr>
  <tr>
    <td class="tg-8hk5">help</td>
    <td class="tg-lni3">Print the help text for the Meerschaum flags.<br><br>The same text is printed for mrsm -h.<br></td>
    <td class="tg-q1tx" colspan="2">None<br></td>
    <td class="tg-j46h">show help</td>
  </tr>
  <tr>
    <td class="tg-yrlk">jobs</td>
    <td class="tg-3y4y">Print a table with information about background jobs.</td>
    <td class="tg-zjt4">• positional arguments<br>  Names of jobs to print (all if omitted).</td>
    <td class="tg-zjt4">• --nopretty</td>
    <td class="tg-tijg">show jobs<br><br>show jobs nice_cat<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">logs</td>
    <td class="tg-lni3">Print the newest lines in the log file as they are printed.<br><br>--nopretty will print the entire contents of the files.<br></td>
    <td class="tg-q1tx">• positional arguments<br>  Names of jobs to print (all if omitted).</td>
    <td class="tg-q1tx">• --nopretty</td>
    <td class="tg-j46h">show logs<br><br>show logs golden_trolley --nopretty<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">modules</td>
    <td class="tg-3y4y">Print all of the currently imported modules.</td>
    <td class="tg-zjt4" colspan="2">None</td>
    <td class="tg-tijg">show modules</td>
  </tr>
  <tr>
    <td class="tg-8hk5">packages</td>
    <td class="tg-lni3">Show the optional Meerschaum dependencies.</td>
    <td class="tg-q1tx">• positional arguments<br>  Dependency groups to print.</td>
    <td class="tg-q1tx">• --nopretty</td>
    <td class="tg-j46h">show packages<br><br>show packages api<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">pipes</td>
    <td class="tg-3y4y">Print a stylized tree of available Meerschaum pipes.<br>Respects global Unicode and ANSI settings.<br></td>
    <td class="tg-zjt4">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-zjt4">• --nopretty<br>• --debug</td>
    <td class="tg-tijg">show pipes<br><br>show pipes -c plugin:noaa<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">plugins</td>
    <td class="tg-lni3">Print all of the installed plugins.<br><br>If the command is "show plugins all",<br>print all of the plugins on the repository.<br><br>If the command is "show plugins [username],"<br>print all of the plugins on the repository from that user.</td>
    <td class="tg-q1tx">• positional arguments<br>  None, "all", or a username<br><br>• -r, --repo, --repository</td>
    <td class="tg-q1tx">• --nopretty<br>• --debug</td>
    <td class="tg-j46h">show plugins<br><br>show plugins all<br><br>show plugins foo -r api:bar<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">rowcounts</td>
    <td class="tg-3y4y">Print the row counts for pipes.<br><br>If 'show rowcounts remote', print the row counts<br>for the remote definitions (only for SQL pipes).<br></td>
    <td class="tg-zjt4">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-zjt4">• positional arguments<br>  None or 'remote'<br>• -w, --workers<br>  How many worker threads to use<br>  when calculating rowcounts.<br></td>
    <td class="tg-tijg">show rowcounts<br><br>show rowcounts -c plugin:foo<br><br>show rowcounts remote<br></td>
  </tr>
  <tr>
    <td class="tg-8hk5">users</td>
    <td class="tg-lni3">Print the registered users on a Meerschaum instance.</td>
    <td class="tg-q1tx" colspan="2">• -i, --instance, --mrsm-instance<br>• --debug<br></td>
    <td class="tg-j46h">show users<br><br>show users -i sql:db<br></td>
  </tr>
  <tr>
    <td class="tg-yrlk">version</td>
    <td class="tg-3y4y">Print the running Meerschaum version.</td>
    <td class="tg-zjt4" colspan="2">• --nopretty</td>
    <td class="tg-tijg">show version<br><br>show version --nopretty<br></td>
  </tr>
  <tr>
    <td class="tg-tv01">sql</td>
    <td class="tg-cl16">{None}</td>
    <td class="tg-2c8s">Open an interactive CLI, read a table, or execute a query.</td>
    <td class="tg-6qkm">• positional arguments<br>  sql {label} {method} {query / table}</td>
    <td class="tg-6qkm">• --nopretty<br>• --gui<br>  Open the resulting dataframe<br>  in a graphical editor.<br><br><br><br></td>
    <td class="tg-491y"># Open a CLI for sql:main<br>sql<br><br># Open a CLI for sql:local<br>sql local<br><br># Read the table 'foo'<br>sql foo<br><br># Read query on 'sql:main'<br>sql "SELECT * FROM foo WHERE id = 1"<br><br># Execute a query on 'sql:local'<br># ('exec' is optional)<br>sql local exec \<br>  "INSERT INTO table (id) VALUES (1)"<br></td>
  </tr>
  <tr>
    <td class="tg-5eak">stack</td>
    <td class="tg-8hk5">See <a href="https://docs.docker.com/engine/reference/commandline/compose/" target="_blank" rel="noopener noreferrer">commands</a>.<br></td>
    <td class="tg-lni3">Manage the Meerschaum stack.<br><br>Alias for docker-compose.<br>Meerschaum flags are ignored.<br></td>
    <td class="tg-q1tx" colspan="2">See <a href="https://docs.docker.com/engine/reference/commandline/compose/" target="_blank" rel="noopener noreferrer">docker-compose documentation</a>.</td>
    <td class="tg-j46h">stack up -d db<br><br>stack ps<br><br>stack down -v</td>
  </tr>
  <tr>
    <td class="tg-tv01" rowspan="4">start</td>
    <td class="tg-dja1">api<br></td>
    <td class="tg-p1sh">Start the web API server.</td>
    <td class="tg-fcjd">• -p, --port<br>  The port to bind to. Defaults to 8000.<br><br>• --host<br>  The host interface to bind to. Defaults to 0.0.0.0.<br><br>• -w, --workers<br>  How many threads to use for the server. Defaults to the number of cores.</td>
    <td class="tg-fcjd">• --no-dash, --nodash<br>  Do not start the web dashboard.<br>  <br>• --no-auth, --noauth<br>  Do not require authentication.<br>  <br>• --production, --gunicorn<br>  Run with web server via gunicorn.</td>
    <td class="tg-8evx">start api -p 8001 --no-dash</td>
  </tr>
  <tr>
    <td class="tg-cl16">gui</td>
    <td class="tg-2c8s">Start the desktop terminal.</td>
    <td class="tg-6qkm">• -p, --port<br>  The port for the webterm server.</td>
    <td class="tg-6qkm">• --debug</td>
    <td class="tg-491y">start gui</td>
  </tr>
  <tr>
    <td class="tg-dja1">jobs</td>
    <td class="tg-p1sh">Start existing jobs or create new jobs.<br><br>You can also create new jobs<br>by adding -d to any command<br>(except stack).</td>
    <td class="tg-fcjd">• positional arguments<br>  The names of jobs or commands for a new job.<br><br>• --name<br>  The name of a new or existing job.<br></td>
    <td class="tg-fcjd">• All other flags are passed to new jobs.</td>
    <td class="tg-8evx">start jobs start api<br><br>start jobs happy_seal<br><br>start jobs --name happy_seal<br><br>start api -d --name my_job<br></td>
  </tr>
  <tr>
    <td class="tg-cl16">webterm</td>
    <td class="tg-2c8s">Start the web terminal.<br><br>Useful for sharing a single Meerschaum<br>instance with a team.<br><br><span style="font-weight:bold">NOTE:</span> This can be a huge security concern!</td>
    <td class="tg-6qkm">• -p, --port<br>  The port to bind to. Defaults to 8765.<br><br>• --host<br>  The host interface to bind to. Defaults to 127.0.0.1.</td>
    <td class="tg-6qkm">• -f, --force<br>  Find the next available port.<br><br>• --nopretty<br><br></td>
    <td class="tg-491y">start webterm<br><br>start webterm -f -h 0.0.0.0</td>
  </tr>
  <tr>
    <td class="tg-5eak">stop</td>
    <td class="tg-yrlk">jobs</td>
    <td class="tg-3y4y">Stop running jobs that were started with -d or start jobs.</td>
    <td class="tg-zjt4">• positional arguments<br>  Names of jobs.<br>  Defaults to all.<br></td>
    <td class="tg-zjt4">• -y, --yes<br>• -f, --force<br>• --noask<br>• --debug<br>• --nopretty</td>
    <td class="tg-tijg">stop jobs -y<br><br>stop jobs happy_bear<br></td>
  </tr>
  <tr>
    <td class="tg-tv01">sync</td>
    <td class="tg-dja1">pipes</td>
    <td class="tg-p1sh">Fetch new data and update your pipes.<br><br>This will execute sync() or fetch() for plugins<br>and SQL queries for SQL pipes.<br><br>The shell interface will print a progress bar<br>and spinner to indicate that a sync is running.<br></td>
    <td class="tg-fcjd">• --loop<br>  Continuously sync pipes.<br><br>• --min-seconds<br>  How many seconds to sleep between laps. Defaults to 1.<br><br>• --timeout, --timeout-seconds<br>  Maximum number of seconds before cancelling a pipe's syncing job. Defaults to 300.<br><br>• --begin<br>  Fetch data newer than this datetime.<br><br>• --end<br>  Fetch data older than this datetime.<br><br>• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-fcjd">• -w, --workers<br>  How many threads to use<br>  to process the queue.<br><br>• --chunksize<br>  Specify the chunksize for syncing<br>  and retrieving data. Defaults to 900.<br><br>• --async, --unblock<br>  Do not wait for a pipe to finish<br>  syncing before continuing.<br><br>• --cache<br>  When syncing pipes, sync to a<br>  local database for later analysis.<br><br>• --debug<br></td>
    <td class="tg-8evx">sync pipes<br><br>sync pipes --loop -c plugin:foo<br><br>sync pipes --timeout 900<br><br>sync pipes --chunksize 2000<br><br>sync pipes -w 1<br><br>sync pipes --async<br><br>sync pipes --begin 2022-01-01\<br>  --end 2022-02-01-01</td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="2">uninstall</td>
    <td class="tg-8hk5">packages</td>
    <td class="tg-lni3">Uninstall Python packages from<br>the Meerschaum virtual environment.</td>
    <td class="tg-q1tx">• positional arguments<br>  List of packages.<br><br>• -A, --sub-args<br>  Additional arguments to pass to pip.</td>
    <td class="tg-q1tx">• --debug</td>
    <td class="tg-j46h">uninstall packages rich pandas</td>
  </tr>
  <tr>
    <td class="tg-yrlk">plugins</td>
    <td class="tg-3y4y">Uninstall local Meerschaum plugins.</td>
    <td class="tg-zjt4">• positional arguments<br>  List of plugins.</td>
    <td class="tg-zjt4">• -y, --yes<br>• --noask<br>• -f, --force<br>• --debug</td>
    <td class="tg-tijg">uninstall plugins noaa</td>
  </tr>
  <tr>
    <td class="tg-tv01" rowspan="3">upgrade</td>
    <td class="tg-cl16">meerschaum, mrsm</td>
    <td class="tg-2c8s">Upgrade to the latest release of Meerschaum, and pull latest images for the stack.</td>
    <td class="tg-6qkm">• positional argument<br>  A dependency group (e.g. 'full' will install meerschaum[full]).<br></td>
    <td class="tg-6qkm">• -y, --yes<br>• --noask<br>• -f, --force<br>• --debug</td>
    <td class="tg-491y">upgrade mrsm<br><br>upgrade meerschaum full</td>
  </tr>
  <tr>
    <td class="tg-dja1">packages</td>
    <td class="tg-p1sh">Upgrade the packages in a dependency group (default 'full'). </td>
    <td class="tg-fcjd">• positional argument<br>  A dependency group.</td>
    <td class="tg-fcjd">• -y, --yes<br>• --noask<br>• -f, --force<br>• --debug</td>
    <td class="tg-8evx">upgrade packages<br><br>upgrade packages docs<br></td>
  </tr>
  <tr>
    <td class="tg-cl16">plugins</td>
    <td class="tg-2c8s">Upgrade installed plugins to the latest versions.<br>If no names are provided, upgrade all plugins.</td>
    <td class="tg-6qkm">• positional arguments<br>  Plugins to upgrade.<br>  Default is all.</td>
    <td class="tg-6qkm">• -y, --yes<br>• --noask<br>• -f, --force<br>• --debug</td>
    <td class="tg-491y">upgrade plugins<br><br>upgrade plugins foo -y<br></td>
  </tr>
  <tr>
    <td class="tg-5eak" rowspan="2">verify</td>
    <td class="tg-8hk5">packages</td>
    <td class="tg-lni3">Verify the versions of installed packages.</td>
    <td class="tg-q1tx" colspan="2">• --debug</td>
    <td class="tg-j46h">verify packages</td>
  </tr>
  <tr>
    <td class="tg-yrlk">pipes</td>
    <td class="tg-3y4y">Verify the contents of pipes using iterative backtracking.<br><br>See my thesis research for <a href="https://github.com/bmeares/syncx" target="_blank" rel="noopener noreferrer">potential strategies</a>.</td>
    <td class="tg-zjt4" colspan="2">• -i, --instance, --mrsm-instance<br>• -c, -C, --connector-keys<br>• -m, -M, --metric-keys<br>• -l, -L, --location-keys<br>• -t, --tags</td>
    <td class="tg-tijg">verify pipes</td>
  </tr>
</tbody>
</table>
