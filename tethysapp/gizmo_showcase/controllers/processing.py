from time import sleep
from django.shortcuts import render, redirect
from django.urls import reverse
from tethys_sdk.routing import controller
from tethys_sdk.gizmos import JobsTable
from .common import docs_endpoint
from django.contrib.auth import get_user_model


@controller
def jobs_table(request):
    """
    Controller for the Jobs Table page.
    """
    from tethys_compute.models import TethysJob
    jobs = TethysJob.objects.filter(label='gizmo_showcase').order_by('id').select_subclasses()

    # Table View
    jobs_table_options = JobsTable(
        jobs=jobs,
        column_fields=('id', 'name', 'description', ('Created At', 'creation_time'), 'extended_properties.parity'),
        hover=True,
        striped=False,
        bordered=False,
        condensed=False,
        monitor_url='gizmo_showcase:jobs_table_results',
        results_url='gizmo_showcase:jobs_table_results',
        refresh_interval=10000,
        show_detailed_status=True,
        delay_loading_status=True,
        actions=[
            'run', 'pause', 'resume', 'resubmit', '|', 'logs', 'monitor', 'results', '|', 'terminate', 'delete', '|',
            dict(
                label='Custom Action',
                callback_or_url=custom_action,
                enabled_callback=lambda self, job_status: self.extended_properties['parity'] == 'even',
                confirmation_message='Custom actions run user-defined custom code. '
                    'This custom action will sleep for 2 seconds and then return. </br></br>'
                    'Custom action can also have customized code to enable/disable the action '
                    '(e.g. this action is only enabled on jobs that have "even" as the extended property "parity"). '
                    '</br></br>'
                    'Additionally, you can specify whether to show the loading overlay when the action is performed. '
                    'This action has the overlay enabled. </br></br>'
             
                    'Are you sure you want to perform a custom action?',
                show_overlay=True
            ),
        ],
    )

    context = {
        'docs_endpoint': docs_endpoint,
        'jobs_table': jobs_table_options
    }
    return render(request, 'gizmo_showcase/jobs_table.html', context)


@controller
def jobs_table_results(request, job_id):
    """Controller for the Jobs Table demo results links."""
    return redirect(reverse('gizmo_showcase:jobs_table'))


@controller(
    name='jobs_table_sample_jobs',
    url='gizmo-showcase/jobs-table/sample-jobs'
)
def create_sample_jobs(request):
    """Controller that creates sample Jobs for the Jobs Table demo."""
    from tethys_compute.models import BasicJob, CondorWorkflow

    def create_job(job_id, description, status, status_msg=None, workflow=False):
        Job = CondorWorkflow if workflow else BasicJob
        user = get_user_model().objects.first()
        job = Job(
            name=f'job_{job_id}',
            user=user,
            description=description,
            label='gizmo_showcase',
            status_message=status_msg,
            _status=status,
            extended_properties={'parity': 'even' if job_id % 2 == 0 else 'odd'}
        )
        job.save()

    for i, (desc, status) in enumerate((
            ('Pending job', 'PEN'),
            ('Submitted job', 'SUB'),
            ('Running job', 'RUN'),
            ('Job error', 'ERR'),
            ('Aborted job', 'ABT'),
            ('Completed job', 'COM'),
    )):
        create_job(i, desc, status)
        create_job(i + 10, desc, status, status_msg=f'{desc} status message')

    create_job(20, 'Running multi-process job with various statuses', 'VAR')
    create_job(21, 'Completed multi-process job with some errors', 'VCP')
    create_job(22, 'Workflow job with multiple nodes.', 'VAR', workflow=True)

    return redirect(reverse('gizmo_showcase:jobs_table'))


def custom_action(self):
    sleep(2)
    self.status = 'My Custom Status'
