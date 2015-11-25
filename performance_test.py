#! /usr/bin/env python
"""
Performance testing script for FTrack. Tests the ftrack_api vs sqlalchemy vs
MySQLdb performing similar queries.

`setup_*` functions are run once and their corresponding `test_*` may be run
multiple times to get timing averages.
"""

import time
import timeit
import gc

global_data = dict(
    FTRACK_SERVER='http://ftrack.luma.ninja',
    FTRACK_APIKEY='51d31b5e-4db9-11e5-a496-e0db550a3928',

    DB_URI='mysql://ftrack:Ywp2YqrqzPdPwrVJ@sv-sql03.luma.mel:3306/ftrack',

    # project to select
    PROJECT_NAME='perf_test_1',
    # sequence to select
    SEQUENCE_NAME='seq_1',

    PROJECTS=10,
    SEQUENCES_PER_PROJECT=10,
    SHOTS_PER_SEQUENCES=50,
    TASKS_PER_SHOT=5,

    # whether to get the first or all results
    RESULT_MODE='all'
)


# -----------------------------------------------------------------------------
# Utils


def echox(cmd, time_update_char='# -update', verbose=True,
          print_comments=True):
    """
    Print and execute a string command optionally printing the times various
    chunks of code took to execute.

    NOTE: all code is executed in the global namespace!

    Parameters
    ----------
    cmd : str
        Command to execute. e.g. 'import time; time.sleep(2)'
    time_update_char : str
        String found in code to print timing updates.
    verbose : bool
        If True, print timing updates at each `time_update_char` found in
        `cmd`.
    print_comments : bool
        Skip over comments lines (ones that begin with '#').
    """

    for chunk in cmd.split(time_update_char):

        chunk = chunk.lstrip().rstrip()

        if verbose:
            print
            start = time.time()

        lines = []
        for line in chunk.split('\n'):
            if verbose:
                if not print_comments and line.startswith('#'):
                    continue
                print '>>> {0}'.format(line)
                lines.append(line)
            else:
                # remove print statements if we're not in verbose mode
                if line.startswith('print'):
                    continue
                lines.append(line)

        exec('\n'.join(lines), globals())

        if verbose:
            print '{0:0.6f} s'.format(time.time() - start)


def execx(stmts='pass', setup='pass', number=1, verbose=False,
          time_update_char='# -update', print_comments=True):
    """
    Entry point for timing a callable. This extracts the source code from the
    passed `stmt` or `setup` callables and execute it using ``timeit``.

    See ``timeit.Timer`` for more details about timing.

    Parameters
    ----------
    stmts : callable or 'pass' or iterable of
        Callables to exceute `number` of times.
    setup : callable or 'pass'
        Callable for initial setup. Called once.
    number : int
        Number of times to execute `stmt`.
    verbose : bool
        If True, print timing updates at each `time_update_char`.
    time_update_char : str
        String found in code to print timing updates.
    print_comments : bool
        Skip over comments lines (ones that begin with '#'). Only used if
        `verbose` is True.

    Returns
    -------
    float
        *Total* time to execute in seconds.
    """

    def convert_to_str(func):
        if isinstance(func, basestring):
            return func
        import inspect

        lines = inspect.getsourcelines(func)[0]

        # remove func definition from lines
        for i, x in enumerate(lines):
            lines.pop(i)
            if '):' in x:
                break

        # strip doc strings out
        if func.__doc__:
            lines = lines[len(func.__doc__.split('\n')):]

        # fix indentation
        for line in lines:
            if line and line != '\n':
                break
        indent = len(line) - len(line.lstrip())
        lines = [x[indent:] if len(x) >= indent else x for x in lines]
        lines.insert(0, 'from __main__ import global_data\n')
        return ''.join(lines)

    def wrap(func):
        if func == 'pass':
            return func
        code = convert_to_str(func)
        if verbose:
            return lambda cmd=code: \
                echox(cmd, time_update_char=time_update_char,
                      print_comments=print_comments)
        else:
            # use only timeit
            return code

    if not isinstance(stmts, (list, tuple)):
        stmts = [stmts]

    setup = wrap(setup)

    result = 0.0
    for stmt in stmts:
        # wrap the callables in echox; this also makes sure the code is ran
        # in the global namespace
        stmt = wrap(stmt)

        # returns the TOTAL time the execute
        result += timeit.Timer(stmt=stmt, setup=setup).timeit(number=number)

    return result


# -----------------------------------------------------------------------------
# Tests


def setup_ftrack():
    """
    Get all shots of a sequence via the ftrack_api.
    """
    # ----------------------------------------------------------------------
    # FTrack

    import os
    # overwrite this so we don't crawl any directories
    os.environ['FTRACK_EVENT_PLUGIN_PATH'] = ''

    import ftrack_api


def test_ftrack_01():
    """
    Get all shots of a sequence via the ftrack_api.
    """
    session = ftrack_api.Session(server_url=global_data['FTRACK_SERVER'],
                                 api_key=global_data['FTRACK_APIKEY'])
    # -update

    r = session.query(
        'select name from Shot where project.name = "{0}" and '
        'parent.name = "{1}"'.format(global_data['PROJECT_NAME'],
                                     global_data['SEQUENCE_NAME']))
    # -update

    if global_data['RESULT_MODE'] == 'all':
        shots = [x['name'] for x in r.all()]
        print "num shots:", len(shots)
    else:
        print "shot name:", r.first()['name']


def test_ftrack_02():
    """
    Test retrieving a all shots.
    """
    session = ftrack_api.Session(server_url=global_data['FTRACK_SERVER'],
                                 api_key=global_data['FTRACK_APIKEY'])
    # -update

    r = session.query('select name from Shot')

    if global_data['RESULT_MODE'] == 'all':
        shots = [x['name'] for x in r.all()]
        print "num shots:", len(shots)
    else:
        print "shot name:", r.first()['name']


def setup_sqlalchemy():
    """
    Get all shots of a sequence with sqlalchemy and some quick models.
    """
    # ----------------------------------------------------------------------
    # SQLAlchemy

    from sqlalchemy.orm import Session, relationship, backref
    from sqlalchemy import (
        create_engine,
        Column, String, Date, Boolean, Float,
        ForeignKey
    )
    from sqlalchemy.ext.declarative import declarative_base
    # -update

    Base = declarative_base()

    class Context(Base):
        __tablename__ = 'context'

        context_type = Column(String)
        id = Column(String, primary_key=True)
        parent_id = Column(String, ForeignKey('context.id'))
        name = Column(String)

        shot = relationship('Shot', uselist=False)
        sequence = relationship('Sequence', uselist=False)
        project = relationship('Project', uselist=False)

        children = relationship(
            'Context', backref=backref('parent', remote_side=[id]))

    class Project(Base):
        __tablename__ = 'show'

        showid = Column(String, ForeignKey('context.id'), primary_key=True)
        fullname = Column(String)
        root = Column(String)
        startdate = Column(Date)
        enddate = Column(Date)
        status = Column(String)
        diskid = Column(String)
        projectschemeid = Column(String)
        thumbid = Column(String)
        isglobal = Column(Boolean)

        context = relationship('Context', uselist=False)

    class _Task(Base):
        __tablename__ = 'task'
        _object_type_id = '*'

        taskid = Column(String, ForeignKey('context.id'), primary_key=True)
        description = Column(String)
        startdate = Column(Date)
        enddate = Column(Date)
        statusid = Column(String)
        typeid = Column(String)
        isopen = Column(Boolean)
        thumbid = Column(String)
        sort = Column(Float)
        object_typeid = Column(String, ForeignKey('object_type.typeid'))
        showid = Column(String, ForeignKey('show.showid'))
        priorityid = Column(String)

        context = relationship('Context', uselist=False)

        __mapper_args__ = {
            'polymorphic_identity': _object_type_id,
            'polymorphic_on': object_typeid
        }

        @property
        def name(self):
            return self.context.name

    class Sequence(_Task):
        _object_type_id = 'e5139355-61da-4c8f-9db4-3abc870166bc'

        __mapper_args__ = {
            'polymorphic_identity': _object_type_id
        }

        project = relationship('Project', uselist=False)

    class Shot(_Task):
        _object_type_id = 'bad911de-3bd6-47b9-8b46-3476e237cb36'

        __mapper_args__ = {
            'polymorphic_identity': _object_type_id
        }


def test_sqlalchemy_01():
    """
    Get all shots of a sequence with sqlalchemy and some quick models.
    """
    engine = create_engine(global_data['DB_URI'])
    # -update
    session = Session(engine)
    # -update

    # FIXME: probably am not doing this as efficiently as we could be...
    subq = session.query(Context)\
        .filter(Context.sequence)\
        .filter_by(name=global_data['SEQUENCE_NAME'])\
        .join(Project, Project.showid == Context.parent_id)\
        .filter(Project.fullname == global_data['PROJECT_NAME']).subquery()

    r = session.query(Context.name)\
        .filter(Context.shot)\
        .join(subq, subq.c.id == Context.parent_id)
    # -update

    if global_data['RESULT_MODE'] == 'all':
        shots = [x[0] for x in r.all()]
        print "num shots:", len(shots)
    else:
        print "shot name:", r.first()[0]


def test_sqlalchemy_02():
    """
    Test retrieving all shots.
    """
    engine = create_engine(global_data['DB_URI'])
    # -update
    session = Session(engine)
    # -update

    r = session.query(Shot)
    # -update

    if global_data['RESULT_MODE'] == 'all':
        shots = list(r.all())
        print "num shots:", len(shots)
    else:
        print "shot name:", r.first()


def setup_mysql():
    """
    Get all shots of a sequence using MySQLdb directly.
    """
    # ----------------------------------------------------------------------
    # MYSQL

    import MySQLdb
    from sqlalchemy.engine.url import make_url
    global_data['DB_URI'] = make_url(global_data['DB_URI'])


def test_mysql_01():
    """
    Get all shots of a sequence using MySQLdb directly.
    """
    session = MySQLdb.connect(
        host=global_data['DB_URI'].host,
        db=global_data['DB_URI'].database,
        user=global_data['DB_URI'].username,
        passwd=global_data['DB_URI'].password).cursor(MySQLdb.cursors.DictCursor)
    # -update

    query = '''
        SELECT context.name FROM task, context
        JOIN (
            SELECT * FROM context
            JOIN `show` ON `show`.showid = context.parent_id
            WHERE context.name = '{0}'
            AND show.fullname = '{1}'
        ) AS anon_1 ON anon_1.id = context.parent_id
        WHERE task.taskid = context.id
        AND task.object_typeid IN ('bad911de-3bd6-47b9-8b46-3476e237cb36')
        '''.format(global_data['SEQUENCE_NAME'], global_data['PROJECT_NAME'])

    session.execute(query)
    r = session
    # -update
    if global_data['RESULT_MODE'] == 'all':
        shots = [x['name'] for x in r.fetchall()]
        print "num shots:", len(shots)
    else:
        print r.fetchone()['name']


def test_mysql_02():
    """
    Test retrieving all shots.
    """
    session = MySQLdb.connect(
        host=global_data['DB_URI'].host,
        db=global_data['DB_URI'].database,
        user=global_data['DB_URI'].username,
        passwd=global_data['DB_URI'].password).cursor(MySQLdb.cursors.DictCursor)
    # -update

    query = '''
        SELECT context.name FROM task, context
        WHERE context.id = task.taskid
        AND task.object_typeid IN ('bad911de-3bd6-47b9-8b46-3476e237cb36')
        '''.format(global_data['PROJECT_NAME'])

    session.execute(query)
    r = session
    # -update
    if global_data['RESULT_MODE'] == 'all':
        shots = [x['name'] for x in r.fetchall()]
        print "num shots:", len(shots)
    else:
        print "shot name:", r.fetchone()['name']


# def setup_luma():
#     """
#     Get all shots of a sequence using MySQLdb directly.
#     """
#     # ----------------------------------------------------------------------
#     # MYSQL
#
#     import MySQLdb
#
#
# def test_luma_01():
#     """
#     Get all shots of a sequence using MySQLdb directly.
#     """
#     session = MySQLdb.connect(
#         host='lumadb',
#         db='luma',
#         user='internal',
#         passwd='dbConnect76').cursor(MySQLdb.cursors.DictCursor)
#     # -update
#
#     query = '''SELECT shots.shot_name AS name FROM shots
#     LEFT JOIN projects USING (id_project)
#     WHERE projects.id_project = "boxcars" AND shots.prefix = "AF"'''
#
#     session.execute(query)
#     r = session
#     # -update
#     if global_data['RESULT_MODE'] == 'all':
#         shots = [x['name'] for x in r.fetchall()]
#         print "num shots:", len(shots)
#     else:
#         print r.fetchone()['name']

# -----------------------------------------------------------------------------
# Setup


def setup_ftrack_create():
    """
    Create test project, sequences and shots using the ftrack_api.
    """
    import os

    # Remove any of our custom stuff from the plugin path.
    # NOTE: Without this we get an additional overhead of ~1 second between
    #       importing the api and constructing the session.
    os.environ['FTRACK_EVENT_PLUGIN_PATH'] = ''

    import ftrack_api


def test_ftrack_create():
    """
    Create test project, sequences and shots using the ftrack_api.
    """
    num_projects = int(global_data['PROJECTS'])
    sequences_per_project = int(global_data['SEQUENCES_PER_PROJECT'])
    shots_per_sequence = int(global_data['SHOTS_PER_SEQUENCES'])
    tasks_per_shot = int(global_data['TASKS_PER_SHOT'])

    print "creating a total of {} projects, {} sequences, {} shots, {} tasks".format(
        num_projects,
        num_projects * sequences_per_project,
        num_projects * sequences_per_project * shots_per_sequence,
        num_projects * sequences_per_project * shots_per_sequence * tasks_per_shot,
    )

    session = ftrack_api.Session(server_url=global_data['FTRACK_SERVER'],
                                 api_key=global_data['FTRACK_APIKEY'])
    # -update

    # Choose project schema.
    project_schema = session.query('ProjectSchema').first()
    # -update

    # -update

    # Retrieve default types.
    default_shot_status = project_schema.get_statuses('Shot')[0]
    default_task_type = project_schema.get_types('Task')[0]
    default_task_status = project_schema.get_statuses(
        'Task', default_task_type['id'])[0]

    counter = 0

    for project_number in range(1, num_projects + 1):
        # Create the project with the chosen schema.
        project_name = 'perf_test_{0}'.format(project_number),
        project = session.create('Project', {
            'name': project_name,
            'full_name': project_name,
            'project_schema': project_schema
        })
        counter += 1
        # Create sequences, shots and tasks.
        for sequence_number in range(1, sequences_per_project + 1):
            sequence = session.create('Sequence', {
                'name': 'seq_{0}'.format(sequence_number),
                'parent': project
            })
            counter += 1
            for shot_number in range(1, shots_per_sequence + 1):
                shot = session.create('Shot', {
                    'name': 'shot_{0:03d}'.format(shot_number),
                    'parent': sequence,
                    'status': default_shot_status
                })
                counter += 1
                for task_number in range(1, tasks_per_shot + 1):
                    session.create('Task', {
                        'name': 'task_{0}'.format(task_number),
                        'parent': shot,
                        'status': default_task_status,
                        'type': default_task_type
                    })
                    counter += 1
                    if counter > 10:
                        # need to commit and garbage collect periodically to
                        # avoid running out of memory
                        session.commit()
                        gc.collect()
                        counter = 0

    # -update
    if counter > 0:
        session.commit()


def get_sequence(project):
    import ftrack_api

    session = ftrack_api.Session(server_url=global_data['FTRACK_SERVER'],
                                 api_key=global_data['FTRACK_APIKEY'])

    seq = session.query('select name from Sequence where project.name = '
                        '"{0}"'.format(project)).first()
    return seq['name']


def cleanup_ftrack_project():
    import ftrack_api
    num_projects = int(global_data['PROJECTS'])

    session = ftrack_api.Session(server_url=global_data['FTRACK_SERVER'],
                                 api_key=global_data['FTRACK_APIKEY'])

    for project_number in range(1, num_projects + 1):
        # Create the project with the chosen schema.
        project_name = 'perf_test_{0}'.format(project_number)
        print project_name
        project = session.query('Project where name = '
                                '"{0}"'.format(project_name)).first()
        if project:
            print "Deleting project '{0}'".format(project_name)
            session.delete(project)
            session.commit()


def gather_tests():
    tests = {
        'setup': (test_ftrack_create, setup_ftrack_create),
        'cleanup': (cleanup_ftrack_project, 'pass')
    }
    for name, value in globals().iteritems():
        parts = name.split('_')
        if len(parts) == 3 and parts[0] == 'test':
            setup_func = globals()['setup_' + parts[1]]
            tests['_'.join(parts[1:])] = (value, setup_func)
    return tests


def get_parser():

    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''

Example
-------

A standard test run, that creates the test data, runs each test, and cleans up
the data

    %(prog)s setup -g SHOTS_PER_SEQUENCES='5' FTRACK_SERVER='http://ftrack.luma.ninja' FTRACK_APIKEY='51d31b5e-4db9-11e5-a496-e0db550a3928'
    %(prog)s sqlalchemy_01 --runs 5 -g DB_URI='mysql://ftrack:Ywp2YqrqzPdPwrVJ@sv-sql03.luma.mel:3306/ftrack'
    %(prog)s sqlalchemy_02 --runs 5 -g DB_URI='mysql://ftrack:Ywp2YqrqzPdPwrVJ@sv-sql03.luma.mel:3306/ftrack'
    %(prog)s mysql_01 --runs 5 -g DB_URI='mysql://ftrack:Ywp2YqrqzPdPwrVJ@sv-sql03.luma.mel:3306/ftrack'
    %(prog)s mysql_02 --runs 5 -g DB_URI='mysql://ftrack:Ywp2YqrqzPdPwrVJ@sv-sql03.luma.mel:3306/ftrack'
    %(prog)s ftrack_01 --runs 5 -g FTRACK_SERVER='http://ftrack.luma.ninja' FTRACK_APIKEY='51d31b5e-4db9-11e5-a496-e0db550a3928'
    %(prog)s ftrack_02 --runs 5 -g FTRACK_SERVER='http://ftrack.luma.ninja' FTRACK_APIKEY='51d31b5e-4db9-11e5-a496-e0db550a3928'
    %(prog)s cleanup -g FTRACK_SERVER='http://ftrack.luma.ninja' FTRACK_APIKEY='51d31b5e-4db9-11e5-a496-e0db550a3928'

Print more verbose output:
    %(prog)s -v

Query an existing project instead of creating a test one:
    %(prog)s -p projectName

    ''')

    parser.add_argument(
        dest='test', type=str, choices=gather_tests().keys(),
        help='Specify the test you wish to run')

    parser.add_argument(
        '-r', '--runs', type=int, default=1,
        help='Number of times to run each test.')

    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Print and time everything that is happening. This is for '
             'debugging and may affect the timing slightly')

    # TODO: add feature to read from .json file
    parser.add_argument(
        '-g', '--globals', metavar="VAR='value'", type=str, nargs='+',
        default=[],
        help="Specify overrides for global variables. "
             "Valid keys are: {0}".format(', '.join(global_data.keys())))

    return parser


def main(argv=None):

    import sys

    if argv is None:
        argv = sys.argv[1:]
    parser = get_parser()
    args = parser.parse_args(argv)

    num = args.runs

    for item in args.globals:
        var, value = item.split('=')
        value = str(value)
        assert var in global_data, \
            "You must provide one of {0}".format(global_data.keys())
        print "Overriding {0} with {1}".format(var, value)
        global_data[var] = value

    print "Running test {0}".format(args.test)
    test_func, setup_func = gather_tests()[args.test]
    elapsed = execx(test_func, setup=setup_func, number=num,
                    verbose=args.verbose)

    print '{0}: Total Average ({1} runs): {2:06f}'.format(args.test, num,
                                                          elapsed / num)



if __name__ == '__main__':
    main()
