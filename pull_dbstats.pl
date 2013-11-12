use strict;
use warnings;

use JSON qw( decode_json ); 
use Data::Dumper;
use List::Util qw(first);

my $begin_date = "2013-11-10T22:00:00Z";
my $end_date   = "2013-11-11T03:00:00Z";
my $field_name  = "call_count"; 
my @db_operations = qw(select update insert delete);


open FILE, "config.json" or die "Couldn't open config file: $!"; 
my $config = decode_json(join("", <FILE>)); 
close FILE;

my $api_key = $config->{'apiKey'};
my @app_names = @{$config->{'apps'}};
my $account_id = $config->{'accountId'};

print "using account_id: $account_id\n";
my $app_data = decode_json(`curl -gH 'x-api-key:${api_key}' https://api.newrelic.com/api/v1/accounts/${account_id}/applications.json`);

my $apps = {};
my %app_names_set = map { $_ => 1 } @app_names;
for my $app (@{$app_data}){
  if(defined $app_names_set{$app->{'name'}}){
    $apps->{$app->{'name'}} = $app->{'id'} ;
  }
}


sub fetch_app_data{
  my ($app_id) = @_;

  my $metrics = decode_json(`curl -s -gH 'x-api-key:${api_key}' 'https://api.newrelic.com/api/v1/applications/${app_id}/metrics.json'`);

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
    my $metric_data = decode_json(`curl -s -gH 'x-api-key:${api_key}' '${metric_data_url}'`);

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
 #   last if $i > 1;
  }

  return $results;
}


my $all_table_names = {};
my $results = {};

for my $app_name(keys %${apps}){
  print "\nFetching data for $app_name\n";
  my $data = fetch_app_data($apps->{$app_name});
  $results->{$app_name} = $data;
  for my $table (keys %${data}){
    $all_table_names->{$table} = 1;
  }
}

open OUTPUT, ">output.txt";
print OUTPUT "\t";
for my $app_name(@app_names){
  print OUTPUT "$app_name\t";
  for(my $i =1; $i < scalar @db_operations; $i++){
    print OUTPUT "\t";
  }
}
print OUTPUT "Total";
print OUTPUT "\n";

print OUTPUT "\t";
for my $app_name(@app_names){
  for(my $i =0; $i < scalar @db_operations; $i++){
    print OUTPUT $db_operations[$i] . "\t";
  }
}
for(my $i =0; $i < scalar @db_operations; $i++){
  print OUTPUT $db_operations[$i] . "\t";
}
print OUTPUT "\n";

for my $table (keys %${all_table_names}){
  my $op_totals = {};
  print OUTPUT "$table\t";
  for my $app_name(@app_names){
    for(my $i =0; $i < scalar @db_operations; $i++){
      my $operation = $db_operations[$i];
      my $value = 0; 
      if(defined $results->{$app_name}->{$table} && defined $results->{$app_name}->{$table}->{$operation}){
        $value = $results->{$app_name}->{$table}->{$operation}; 
      }
      $op_totals->{$operation} += $value;
      print OUTPUT ($value == 0 ? "" : $value) . "\t";
    }
  }
  for(my $i =0; $i < scalar @db_operations; $i++){
    print OUTPUT $op_totals->{$db_operations[$i]} . "\t"; 
  } 
  print OUTPUT "\n";
}
