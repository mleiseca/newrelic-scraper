use strict;
use warnings;

use JSON qw( decode_json ); 
use Data::Dumper;
use List::Util qw(first);

my $begin_date = "2013-11-10T20:00:00Z";
my $end_date   = "2013-11-10T23:00:00Z";
my $metric_name = "Database/order_event_log/insert";
my $field_name  = "call_count"; 

my @db_operations = qw(select update insert delete);

open OUTPUT, ">output.txt";
my $metrics = decode_json(`curl -gH 'x-api-key:${api_key}' 'https://api.newrelic.com/api/v1/applications/${app_id}/metrics.json'`);

my $db_tables_to_operations = {};


for my $metric_type (@${metrics}){
  if($metric_type->{'name'} =~ /^Database\/(.*?)\/(.*)/){
     my $table = $1;
     my $operation = $2;
     my $ops_for_table = $db_tables_to_operations->{$table} || [];
     push (@{$ops_for_table}, $operation); 
     $db_tables_to_operations->{$table} = $ops_for_table;
  }
}



my $results = {};
my $table_count = scalar keys %${db_tables_to_operations};
print "Found tables numbering : $table_count \n";
my $i = 0;
for my $table(keys %${db_tables_to_operations}){ 
  $i++;
  my @metrics_params = ();

  for my $operation(@{$db_tables_to_operations->{$table}}){
    my $metric_name = "metrics[]=Database/$table/$operation";
    push @metrics_params, $metric_name ; 
  }
  my $all_metrics = join("&", @metrics_params);
  my $metric_data_url = "https://api.newrelic.com/api/v1/applications/${app_id}/data.json?begin=${begin_date}&end=${end_date}&${all_metrics}&field=${field_name}";
  print "($i/$table_count)...fetching from $metric_data_url\n";
  my $metric_data = decode_json(`curl -gH 'x-api-key:${api_key}' '${metric_data_url}'`);
  

  my $table_results = {};
  for my $period (@${metric_data}){
    if($period->{'name'} =~ /^Database\/(.*?)\/(.*)/){
      my $table = $1;
      my $operation = $2;
      if(! defined $table_results->{$operation} || $period->{$field_name} > $table_results->{$operation}){
        $table_results->{$operation} = $period->{$field_name};
      }  
    } 
  }

  $results->{$table} = $table_results;
}


print OUTPUT "\t". join("\t", @db_operations) . "\n";
for my $table(keys %${results}){
  my $total_ops = 0;
  print OUTPUT "$table\t";
  for($i =0; $i < scalar @db_operations; $i++){
    my $value = $results->{$table}->{$db_operations[$i]} || 0;

    print OUTPUT ($value == 0 ? "" : $value) . "\t";

    $total_ops+= $value;
  }
  print OUTPUT "$total_ops\n";
}
