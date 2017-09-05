import functools

from django.db import transaction

from gecko.flow.start import STATUS


def celery_task(func):

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        flow_task_strref = (
            kwargs.pop('flow_task_strref')
            if 'flow_task_strref' in kwargs else args[0]
        )
        process_pk = (
            kwargs.pop('process_pk')
            if 'process_pk' in kwargs else args[1]
        )
        task_pk = kwargs.pop('task_pk') if 'task_pk' in kwargs else args[2]
        flow_task = import_task_by_ref(flow_task_strref)

        lock = flow_task.flow_class.lock_impl(flow_task.flow_class.instance)

        # start
        with transaction.atomic(), lock(flow_task.flow_class, process_pk):
            try:
                task = flow_task.flow_class.task_class.objects.get(
                    pk=task_pk,
                    process_id=process_pk
                )
                if task.status == STATUS.CANCELED:
                    return
            except flow_task.flow_class.task_class.DoesNotExist:
                # There was rollback on job task created transaction,
                # we don't need to do the job
                return
            else:
                activation = flow_task.activation_class()
                activation.initialize(flow_task, task)
                if task.status == STATUS.SCHEDULED:
                    activation.start()
                else:
                    activation.restart()

        # execute
        try:
            result = func(activation, **kwargs)
        except Exception as exc:
            # mark as error
            with transaction.atomic(), lock(flow_task.flow_class, process_pk):
                task = flow_task.flow_class.task_class.objects.get(
                    pk=task_pk,
                    process_id=process_pk
                )
                activation = flow_task.activation_class()
                activation.initialize(flow_task, task)
                activation.error(comments="{}\n{}".format(exc, traceback.format_exc()))
            raise
        else:
            # mark as done
            with transaction.atomic(), lock(flow_task.flow_class, process_pk):
                task = flow_task.flow_class.task_class.objects.get(pk=task_pk, process_id=process_pk)
                activation = flow_task.activation_class()
                activation.initialize(flow_task, task)
                activation.done()

            return result

    return _wrapper
