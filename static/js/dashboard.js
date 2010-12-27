$(document).ready(function() {
  $('#unique_contributors_per_project_table').dataTable({
    "bPaginate": false,
    "bLengthChange": false,
    "bFilter": false});
  $('#contributors_by_organization_table').dataTable({
    "bPaginate": false,
    "bLengthChange": false,
    "bFilter": false});
  $('#fixed_issues_table').dataTable({
    "bPaginate": false,
    "bLengthChange": false,
    "bFilter": false});
} );
